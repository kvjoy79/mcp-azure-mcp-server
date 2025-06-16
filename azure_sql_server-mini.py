import os
import json
from typing import Any, Optional
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.mgmt.sql import SqlManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.sql.models import CheckNameAvailabilityRequest
from mcp.server.fastmcp import FastMCP
import dotenv

# Load environment variables from .env file if it exists
dotenv.load_dotenv()

# Ensure required environment variables are set
@dataclass
class AzureContext:
    """Azure management context"""
    sql_client: Optional[SqlManagementClient] = None
    resource_client: Optional[ResourceManagementClient] = None
    subscription_id: Optional[str] = None
    credential: Optional[Any] = None


@asynccontextmanager
async def azure_lifespan(server: FastMCP) -> AsyncIterator[AzureContext]:
    """Manage Azure client lifecycle"""
    context = AzureContext()
    
    # Get Azure credentials and subscription
    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    
    if not subscription_id:
        print("Warning: AZURE_SUBSCRIPTION_ID not set.")
        yield context
        return
    
    try:
        # Initialize credentials
        if all([tenant_id, client_id, client_secret]):
            # Service Principal authentication
            context.credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
            print("Using Service Principal authentication")
        else:
            # Default credential (Managed Identity, Azure CLI, etc.)
            context.credential = DefaultAzureCredential()
            print("Using Default Azure credential")
        
        # Initialize clients
        context.subscription_id = subscription_id
        context.sql_client = SqlManagementClient(
            credential=context.credential,
            subscription_id=subscription_id
        )
        context.resource_client = ResourceManagementClient(
            credential=context.credential,
            subscription_id=subscription_id
        )
        
        print(f"Connected to Azure subscription: {subscription_id}")
        
    except Exception as e:
        print(f"Warning: Could not initialize Azure clients: {e}")
    
    try:
        yield context
    finally:
        # Cleanup if needed
        print("Azure client context closed")


# Create MCP server with Azure lifecycle management
mcp = FastMCP(
    name="Azure SQL Management Server",
    dependencies=[
        "azure-identity", 
        "azure-mgmt-sql", 
        "azure-mgmt-resource",
        "azure-core"
    ],
    lifespan=azure_lifespan
)


@mcp.tool()
def list_resource_groups() -> str:
    """List all resource groups in the subscription"""
    # Get the Azure context from the lifespan context
    ctx = mcp.get_context()
    azure_ctx = ctx.request_context.lifespan_context

    if not azure_ctx.resource_client:
        return "Error: Azure client not initialized. Please check your credentials."
    
    try:
        resource_groups = list(azure_ctx.resource_client.resource_groups.list())
        
        if not resource_groups:
            return "No resource groups found in the subscription."
        
        result = ["RESOURCE GROUPS", "=" * 50]
        for rg in resource_groups:
            result.append(f"Name: {rg.name}")
            result.append(f"Location: {rg.location}")
            result.append(f"Tags: {rg.tags or 'None'}")
            result.append("-" * 30)
        
        return "\n".join(result)
        
    except Exception as e:
        return f"Failed to list resource groups: {str(e)}"


@mcp.tool()
def create_resource_group(name: str, location: str, tags: Optional[str] = None) -> str:
    """
    Create a new resource group
    
    Args:
        name: Name of the resource group
        location: Azure region (e.g., 'East US', 'West Europe')
        tags: Optional tags as JSON string (e.g., '{"Environment": "Dev", "Project": "MyApp"}')
    """
    ctx = mcp.get_context()
    azure_ctx = ctx.request_context.lifespan_context
    
    if not azure_ctx.resource_client:
        return "Error: Azure client not initialized. Please check your credentials."
    
    try:
        # Parse tags if provided
        tag_dict = {}
        if tags:
            try:
                tag_dict = json.loads(tags)
            except json.JSONDecodeError:
                return "Error: Tags must be valid JSON format"
        
        # Create resource group
        rg_params = {
            'location': location,
            'tags': tag_dict
        }
        
        result = azure_ctx.resource_client.resource_groups.create_or_update(
            resource_group_name=name,
            parameters=rg_params
        )
        
        return f"Resource group '{name}' created successfully in {location}"
        
    except Exception as e:
        return f"Failed to create resource group: {str(e)}"


@mcp.tool()
def list_sql_servers(resource_group: Optional[str] = None) -> str:
    """
    List SQL servers in subscription or specific resource group
    
    Args:
        resource_group: Optional resource group name to filter servers
    """
    ctx = mcp.get_context()
    azure_ctx = ctx.request_context.lifespan_context
    
    if not azure_ctx.sql_client:
        return "Error: Azure SQL client not initialized. Please check your credentials."
    
    try:
        if resource_group:
            servers = list(azure_ctx.sql_client.servers.list_by_resource_group(resource_group))
        else:
            servers = list(azure_ctx.sql_client.servers.list())
        
        if not servers:
            location_msg = f" in resource group '{resource_group}'" if resource_group else ""
            return f"No SQL servers found{location_msg}."
        
        result = ["SQL SERVERS", "=" * 50]
        for server in servers:
            result.append(f"Name: {server.name}")
            result.append(f"Location: {server.location}")
            result.append(f"Resource Group: {server.id.split('/')[4]}")
            result.append(f"State: {server.state}")
            result.append(f"Version: {server.version}")
            result.append(f"Admin Login: {server.administrator_login}")
            result.append(f"Fully Qualified Domain Name: {server.fully_qualified_domain_name}")
            result.append("-" * 50)
        
        return "\n".join(result)
        
    except Exception as e:
        return f"Failed to list SQL servers: {str(e)}"


@mcp.tool()
def create_sql_server(
    resource_group: str,
    server_name: str,
    location: str,
    admin_login: str,
    admin_password: str,
    version: str = "12.0"
) -> str:
    """
    Create a new Azure SQL Server

    Args:
        resource_group: Name of the resource group.
        server_name: Globally unique SQL server name (lowercase letters and numbers only, 3-63 chars).
        location: Azure region (e.g., 'eastus', 'westeurope').
        admin_login: Administrator login name.
        admin_password: Administrator password.
        version: SQL Server version (default: 12.0).
    """
    ctx = mcp.get_context()
    azure_ctx = ctx.request_context.lifespan_context
    
    if not azure_ctx.sql_client:
        return "Error: Azure SQL client not initialized. Please check your credentials."
    
    try:
        # Check if server name is available
        availability_request = CheckNameAvailabilityRequest(name=server_name, type="Microsoft.Sql/servers")
        availability = azure_ctx.sql_client.servers.check_name_availability(
            parameters=availability_request
        )
        
        if not availability.available:
            return f"Server name '{server_name}' is not available: {availability.message}"
        
        # Server parameters
        server_params = {
            'location': location,
            'version': version,
            'administrator_login': admin_login,
            'administrator_login_password': admin_password
        }
        
        # Create server (this is a long-running operation)
        print(f"Creating SQL server '{server_name}'... This may take several minutes.")
        
        operation = azure_ctx.sql_client.servers.begin_create_or_update(
            resource_group_name=resource_group,
            server_name=server_name,
            parameters=server_params
        )
        
        # Wait for completion
        server = operation.result()
        
        return f"""SQL Server created successfully!
                    Name: {server.name}
                    Location: {server.location}
                    State: {server.state}
                    FQDN: {server.fully_qualified_domain_name}
                    Admin Login: {server.administrator_login}

                    Next steps:
                    1. Configure firewall rules to allow connections
                    2. Create databases on this server"""
        
    except Exception as e:
        return f"Failed to create SQL server: {str(e)}"


@mcp.tool()
def list_databases(resource_group: str, server_name: str) -> str:
    """
    List databases on a SQL server
    
    Args:
        resource_group: Resource group name
        server_name: SQL server name
    """
    ctx = mcp.get_context()
    azure_ctx = ctx.request_context.lifespan_context
    
    if not azure_ctx.sql_client:
        return "Error: Azure SQL client not initialized. Please check your credentials."
    
    try:
        databases = list(azure_ctx.sql_client.databases.list_by_server(
            resource_group_name=resource_group,
            server_name=server_name
        ))
        
        if not databases:
            return f"No databases found on server '{server_name}'."
        
        result = [f"DATABASES ON {server_name}", "=" * 50]
        for db in databases:
            result.append(f"Name: {db.name}")
            result.append(f"Status: {db.status}")
            result.append(f"Edition: {db.sku.tier if db.sku else 'N/A'}")
            result.append(f"Service Objective: {db.sku.name if db.sku else (db.current_service_objective_name or 'N/A')}")
            result.append(f"Max Size: {db.max_size_bytes or 'N/A'}")
            result.append(f"Creation Date: {db.creation_date or 'N/A'}")
            result.append("-" * 30)
        
        return "\n".join(result)
        
    except Exception as e:
        return f"Failed to list databases: {str(e)}"


@mcp.tool()
def create_database(
    resource_group: str,
    server_name: str,
    database_name: str,
    edition: str = "Basic",
    service_objective: str = "Basic"
) -> str:
    """
    Create a new database on a SQL server
    
    Args:
        resource_group: Resource group name
        server_name: SQL server name
        database_name: Database name
        edition: Database edition (Basic, Standard, Premium, GeneralPurpose, BusinessCritical)
        service_objective: Service level objective (Basic, S0, S1, P1, GP_Gen5_2, etc.)
    """
    ctx = mcp.get_context()
    azure_ctx = ctx.request_context.lifespan_context
    
    if not azure_ctx.sql_client:
        return "Error: Azure SQL client not initialized. Please check your credentials."
    
    try:
        # Get the server's location
        server_details = azure_ctx.sql_client.servers.get(
            resource_group_name=resource_group,
            server_name=server_name
        )
        server_location = server_details.location
        
        # Database parameters
        db_params = {
            'location': server_location,
            'sku': {
                'name': service_objective,
                'tier': edition 
            }
        }
        
        print(f"Creating database '{database_name}' on server '{server_name}' in location '{server_location}'...")
        
        # Create database
        operation = azure_ctx.sql_client.databases.begin_create_or_update(
            resource_group_name=resource_group,
            server_name=server_name,
            database_name=database_name,
            parameters=db_params
        )
        
        # Wait for completion
        database = operation.result()
        
        return f"""Database created successfully!
                Name: {database.name}
                Location: {database.location}
                Edition: {database.sku.tier if database.sku else 'N/A'}
                Service Objective: {database.sku.name if database.sku else 'N/A'}
                Status: {database.status}
                Creation Date: {database.creation_date}

                Connection string format:
                Server={server_name}.database.windows.net;Database={database_name};"""
        
    except ResourceNotFoundError:
        return f"SQL Server '{server_name}' not found in resource group '{resource_group}'."
    except Exception as e:
        return f"Failed to create database: {str(e)}"


@mcp.resource("azure://subscription")
def get_subscription_info() -> str:
    """Get Azure subscription information"""
    ctx = mcp.get_context()
    azure_ctx = ctx.request_context.lifespan_context
    
    if not azure_ctx.subscription_id:
        return "No Azure subscription configured"
    
    return f"Azure Subscription ID: {azure_ctx.subscription_id}"


@mcp.resource("azure://servers")
def get_all_servers() -> str:
    """Get all SQL servers as a resource"""
    return list_sql_servers()


@mcp.prompt()
def database_creation_prompt(
    purpose: str,
    expected_load: str = "low",
    data_size: str = "small"
) -> str:
    """
    Generate a database creation guidance prompt
    
    Args:
        purpose: Purpose of the database (e.g., "web application", "analytics", "testing")
        expected_load: Expected load (low, medium, high)
        data_size: Expected data size (small, medium, large)
    """
    
    # Suggest editions and service objectives based on requirements
    suggestions = {
        ("low", "small"): ("Basic", "Basic"),
        ("low", "medium"): ("Standard", "S1"),
        ("medium", "medium"): ("Standard", "S2"),
        ("high", "large"): ("Premium", "P1"),
    }
    
    key = (expected_load.lower(), data_size.lower())
    edition, service_obj = suggestions.get(key, ("Standard", "S1"))
    
    return f"""
            I need to create an Azure SQL database for: {purpose}

            Based on your requirements:
            - Expected load: {expected_load}
            - Data size: {data_size}

            Recommended configuration:
            - Edition: {edition}
            - Service Objective: {service_obj}

            Steps to create:
            1. Ensure you have a resource group
            2. Create or use an existing SQL server
            3. Create the database with recommended settings
            4. Configure firewall rules for access
            5. Set up connection strings
            """


if __name__ == "__main__":
    # Run the MCP server    
    mcp.run(transport="streamable-http")