#!/usr/bin/env python3
"""
Test script for the feature service tool to verify it works with individual parameters.
"""

import sys
import os
import logging

# Configure logging to see INFO level messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Add the src directory to the Python path
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'src'))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

import tecton
from tecton_mcp.tools.feature_service_tool_library import (
    extract_join_keys_map_from_feature_service,
    extract_request_context_map_from_feature_service,
    _create_feature_service_tool_function
)

def test_fraud_detection_feature_service():
    """Test the user info autogen feature service tool with sample data."""
    
    print("Testing user info autogen feature service tool...")
    
    # Get the current workspace and feature service
    try:
        workspace_name = tecton.get_current_workspace()
        ws = tecton.get_workspace(workspace_name)
        fs = ws.get_feature_service('user_info_autogen_fs')
        
        print(f"✓ Found feature service: {fs.name}")
        print(f"✓ Workspace: {workspace_name}")
        
        # Extract join keys and request context
        join_keys_info = extract_join_keys_map_from_feature_service(fs)
        request_context_info = extract_request_context_map_from_feature_service(fs)
        
        print(f"✓ Join keys: {list(join_keys_info.keys())}")
        print(f"✓ Request context: {list(request_context_info.keys())}")
        
        # Get the cluster URL from Tecton's configuration
        from tecton._internals.utils import cluster_url
        tecton_cluster_url = cluster_url()
        print(f"✓ Cluster URL: {tecton_cluster_url}")
        
        # Create the tool function
        tool_function = _create_feature_service_tool_function(
            workspace_name, 
            'user_info_autogen_fs',
            join_keys_info,
            request_context_info,
            tecton_cluster_url
        )
        
        print(f"✓ Created tool function: {tool_function.__name__}")
        print(f"✓ Function annotations: {tool_function.__annotations__}")
        
        # Test with sample data
        print("\n" + "="*50)
        print("TESTING WITH SAMPLE DATA")
        print("="*50)
        
        # Sample test data - only user_id for this feature service
        test_data = {
            'user_id': 'C1000262126'  # Using a realistic user ID
        }
        
        print(f"Test data: {test_data}")
        
        # Call the function with individual parameters
        try:
            result = tool_function(
                user_id=test_data['user_id']
            )
            
            print(f"✓ Tool function executed successfully!")
            print(f"✓ Result type: {type(result)}")
            print(f"✓ Result: {result}")
            
        except Exception as e:
            print(f"✗ Error calling tool function: {e}")
            print(f"  Error type: {type(e)}")
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        print(f"✗ Error setting up test: {e}")
        print(f"  Error type: {type(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fraud_detection_feature_service() 