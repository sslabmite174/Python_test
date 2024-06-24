import os
from convert_fot import main as convert_fot_main  # Import the main function from convert_fot

def test_convert_fot():
    # Input file path
    mf4_file = '/home/kpit/Downloads/convert_fot/20211201_005456_GPS.mf4'
    # Set the OUTPUT_PATH
    OUTPUT_PATH = '/home/kpit/Downloads/convert_fot/output'
    FOT_PATH = '/home/kpit/Downloads/convert_fot'
    # Create the output directory if it doesn't exist
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)

    # Convert the MF4 file using convert_fot.main
    csv_file_path = convert_fot_main(mf4_file,FOT_PATH,OUTPUT_PATH)

    if csv_file_path:
        print(f"CSV file successfully created at: {csv_file_path}")
    else:
        print(f"Failed to convert {mf4_file} to CSV.")

if __name__ == "__main__":
    test_convert_fot()