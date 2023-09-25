import cv2
import json
import time
import os
import math
import pyodbc


class img_stream_process:
    def __init__(self, camera_id, geo_location, output_folder,output_folder1, config_filename):
        self.camera_id = camera_id
        self.geo_location = geo_location
        self.output_folder = output_folder
        self.frame_id = 0
        self.cap1 = cv2.VideoCapture(camera_id[0])
        self.cap2 = cv2.VideoCapture(camera_id[1])
        self.config_filename = config_filename
        self.batch_duration = self.read_config_duration()[0]
        self.batch_size = self.read_config_duration()[1]
        

        # Create the output folder
        os.makedirs(output_folder, exist_ok=True)
        os.makedirs(output_folder1, exist_ok=True)


        #JSON data list and batch list
        self.json_data = []
        self.batches = []

    def read_config_duration(self):
        # Read the duration from the config file
        with open(self.config_filename, "r") as config_file:
            config_data = json.load(config_file)
            duration = config_data.get("duration")
            batch_size = config_data.get("batch_size")
        return duration,batch_size

    def video_capture(self):
        # Check if the video stream was opened successfully
        if not self.cap1.isOpened():
            print("Error: Could not open video source.")
            exit()
        if not self.cap2.isOpened():
            print("Error: Could not open video source.")
            exit()

    def frame_processing(self):
        # Set the desired frames per second (fps)
        desired_fps = 25

        frame_count = 0
        frame_to_save1 = None  
        frame_to_save2 = None

        while True:
            # Read a frame from the video stream
            ret1, frame1 = self.cap1.read()
            ret2,frame2 = self.cap2.read()

            # Check if the frame was successfully read
            if not ret1:
                print("Error: Could not read frame.")
                break
            if not ret2:
                print("Error: Could not read frame.")
                break

            frame_count += 1

            # Save one frame per second as an image file
            if frame_count % desired_fps == 0:
                outframe_id = (frame_count // desired_fps)
                frame_to_save1 = frame1  # Save the frame to be reused
                frame_to_save2 = frame2
                frame_filename1 = os.path.join(self.output_folder, f"frame_{outframe_id}.jpg")
                frame_filename2 = os.path.join(self.output_folder1, f"frame_{outframe_id}.jpg")

                cv2.imwrite(frame_filename1, frame_to_save1)  # Save the frame as an image
                cv2.imwrite(frame_filename2, frame_to_save2)  # Save the frame as an image


                # Append frame info to the JSON data list
                frame_info1 = {
                    "camera_id": self.camera_id[0],
                    "frame_id": outframe_id,
                    "geo_location": self.geo_location,
                    "image_path": frame_filename1
                }
                frame_info2 = {
                    "camera_id": self.camera_id[1],
                    "frame_id": outframe_id,
                    "geo_location": self.geo_location,
                    "image_path": frame_filename2
                }
                self.json_data.append(frame_info1)
                self.json_data.append(frame_info2)


            # Reuse the saved frame for the next 24 frames within the same second
            if frame_to_save1 is not None:
                cv2.imshow("Frame", frame_to_save1)

            # Exit the loop when the 'q' key is pressed
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
            # Release the video capture object and close any open windows
        self.cap.release()
        cv2.destroyAllWindows()

    def batch_frames(self):     
    # Calculate the number of batches.
        num_batches = math.ceil(self.batch_duration / self.batch_size)

    # Create a dictionary for each batch.
        #batches = {}
        for i in range(num_batches):
            batch_id = i + 1

        # Calculate the starting and ending frame IDs for the batch.
            starting_frame_id = batch_id * self.batch_size - self.batch_size + 1
            ending_frame_id = min(starting_frame_id + self.batch_size - 1, self.batch_duration)

        # Calculate the timestamp of the batch.
            timestamp = starting_frame_id / self.batch_duration
        #timestamp = time.time()

        # Add the batch to the dictionary.
            batch = {
            "batch_id": batch_id,
            "starting_frame_id": starting_frame_id,
            "ending_frame_id": ending_frame_id,
            "timestamp": timestamp
            }
            self.batches.append(batch)
            return self.batches
    def sqlserver_db(self):
        conn = pyodbc.connect(Driver='{ODBC Driver 18 for SQL Server}',Server="covid19-srv01.database.windows.net",Database="covid-db",Uid="myadmin",Pwd="Hooked@8",Encrypt="yes",TrustServerCertificate="no",Connection_Timeout=30)
        cursor = conn.cursor()
        print(self.batches[0].values())

        
    def save_json_data(self, json_filename):
        # Write JSON data to a JSON file
        with open(json_filename, "w") as json_file:
            json.dump(self.json_data, json_file, indent=4)

    def save_batch_info(self, batch_filename):
        with open(batch_filename, "w") as batch_file:
            json.dump(self.batches, batch_file, indent=4)

if __name__ == "__main__":
    camera_id = 0
    geo_location = "latitude: 123.456, longitude: 789.012"
    output_folder = "output_frames"
    output_folder1 = "outframes"
    config_filename = "config.json"
    json_filename = "frame_info.json"
    batch_filename = "batch_info.json"

    processor = img_stream_process(camera_id, geo_location, output_folder,output_folder1, config_filename)
    processor.video_capture()
    processor.frame_processing()
    processor.batch_frames()
    processor.sqlserver_db()
    processor.save_json_data(json_filename)
    processor.save_batch_info(batch_filename)


    print("Frame processing and batching complete. JSON data saved to:", json_filename)
    print("Batch information saved to:", batch_filename)
