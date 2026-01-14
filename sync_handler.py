from flask import jsonify
from services.folder_structure_service import create_folders
from services.downloading_csv_service import parse_and_load_vehicle_data, compare_buffer_and_data_csv, get_drive_service, find_folder_by_name, find_file_by_name
from services.copying_images_service import copy_images_from_buffer
from services.transfer_data_service import transfer_buffer_to_data, clear_csv_file

def handle_sync():
    folder_ids = None
    changes_detected = False
    
    # Step 1: Ensure folder structure exists
    try:
        folder_ids = create_folders()
        print("Step 1 complete: Folder structure created/verified")
    except Exception as e:
        print(f"Error creating folder structure: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500
    
    # Step 2: Parse and load vehicle data to buffer.csv
    try:
        parse_and_load_vehicle_data()
        print("Step 2 complete: Vehicle data loaded to buffer.csv")
    except Exception as e:
        print(f"Error parsing and loading vehicle data: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500
    
    # Check if buffer.csv is same as data.csv (first 3 columns only)
    try:
        if compare_buffer_and_data_csv():
            print("Changes detected: No")
            
            # Clear buffer.csv since no changes detected
            try:
                service = get_drive_service()
                root_folder_id = find_folder_by_name(service, 'Revive Auctions')
                buffer_csv_id = find_file_by_name(service, 'buffer.csv', root_folder_id)
                if buffer_csv_id:
                    clear_csv_file(service, buffer_csv_id)
            except Exception as e:
                print(f"Warning: Failed to clear buffer.csv: {str(e)}")
            
            return jsonify({"success": True, "changes": False}), 200
        else:
            changes_detected = True
            print("Changes detected: Yes")
    except Exception as e:
        print(f"Error comparing CSVs: {str(e)}")
        changes_detected = True
        print("Changes detected: Yes (comparison failed, proceeding)")
    
    # Step 3: Copy images from drive links to buffer folders
    try:
        results = copy_images_from_buffer()
        print("Step 3 complete: Images copied to buffer folders")
    except Exception as e:
        print(f"Error copying images: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500
    
    # Step 4: Transfer buffer.csv to data.csv with new drive links
    try:
        transfer_buffer_to_data()
        print("Step 4 complete: Data transferred to data.csv")
    except Exception as e:
        print(f"Error transferring data: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500
    
    return jsonify({"success": True, "changes": changes_detected}), 200



        
