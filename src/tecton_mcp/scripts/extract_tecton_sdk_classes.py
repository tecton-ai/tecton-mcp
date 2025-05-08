from tecton_mcp.utils.sdk_introspector import get_sdk_definitions, format_sdk_definitions

def main():
    details, all_defs = get_sdk_definitions()
    output_string = format_sdk_definitions(details, all_defs)
    print(output_string)

if __name__ == "__main__":
    main() 