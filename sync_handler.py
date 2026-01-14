from services.folder_structure_service import create_folders
from services.downloading_csv_service import parse_and_load_vehicle_data, compare_buffer_and_data_csv, get_drive_service, find_folder_by_name, find_file_by_name
from services.copying_images_service import copy_images_from_buffer
from services.transfer_data_service import transfer_buffer_to_data, clear_csv_file

def handle_sync_background(status_dict):
    """
    Handle sync in background thread with status updates.
    Returns result dict with success and changes info.
    """
    folder_ids = None
    changes_detected = False
    
    # Step 1: Ensure folder structure exists
    try:
        status_dict["current_step"] = "Step 1: Creating/verifying folder structure"
        folder_ids = create_folders()
        print("Step 1 complete: Folder structure created/verified")
    except Exception as e:
        print(f"Error creating folder structure: {str(e)}")
        raise Exception(f"Folder structure error: {str(e)}")
    
    # Step 2: Parse and load vehicle data to buffer.csv
    try:
        status_dict["current_step"] = "Step 2: Loading vehicle data to buffer"
        parse_and_load_vehicle_data()
        print("Step 2 complete: Vehicle data loaded to buffer.csv")
    except Exception as e:
        print(f"Error parsing and loading vehicle data: {str(e)}")
        raise Exception(f"Data parsing error: {str(e)}")
    
    # Check if buffer.csv is same as data.csv (first 3 columns only)
    try:
        status_dict["current_step"] = "Comparing data for changes"
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
            
            return {"success": True, "changes": False}
        else:
            changes_detected = True
            print("Changes detected: Yes")
    except Exception as e:
        print(f"Error comparing CSVs: {str(e)}")
        changes_detected = True
        print("Changes detected: Yes (comparison failed, proceeding)")
    
    # Step 3: Copy images from drive links to buffer folders
    try:
        status_dict["current_step"] = "Step 3: Copying images to buffer folders"
        results = copy_images_from_buffer()
        print("Step 3 complete: Images copied to buffer folders")
    except Exception as e:
        print(f"Error copying images: {str(e)}")
        raise Exception(f"Image copying error: {str(e)}")
    
    # Step 4: Transfer buffer.csv to data.csv with new drive links
    try:
        status_dict["current_step"] = "Step 4: Transferring data to data.csv"
        transfer_buffer_to_data()
        print("Step 4 complete: Data transferred to data.csv")
    except Exception as e:
        print(f"Error transferring data: {str(e)}")
        raise Exception(f"Data transfer error: {str(e)}")
    
    return {"success": True, "changes": changes_detected}



        
