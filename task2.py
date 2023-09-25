import cv2
import json
import time
import os
import math
import pyodbc


class img_stream_process:
    def __init__(self, camera_id, geo_location, output_folder, config_filename):
        self.camera_id = camera_id
        self.geo_location = geo_location
        self.output_folder = output_folder
        self.frame_id = 0
        self.cap = cv2.VideoCapture(camera_id)
        self.config_filename = config_filename
        #video duration
        self.batch_duration = int(input())
        #batch size
        self.batch_size = int(input())
        

        # Create the output folder
        os.makedirs(output_folder, exist_ok=True)

        #JSON data list and batch list
        self.json_data = []
        self.batches = []

    def video_capture(self):
        # Check if the video stream was opened successfully
        if not self.cap.isOpened():
            print("Error: Could not open video source.")
            exit()

    def frame_processing(self):
        # Set the desired frames per second (fps)
        desired_fps = 25

        frame_count = 0
        frame_to_save = None

        while True:
            # Read a frame from the video stream
            ret, frame = self.cap.read()

            # Check if the frame was successfully read
            if not ret:
                print("Error: Could not read frame.")
                break

            frame_count += 1

            # Save one frame per second as an image file
            if frame_count % desired_fps == 0:
                outframe_id = (frame_count // desired_fps)
                frame_to_save = frame  # Save the frame to be reused
                frame_filename = os.path.join(self.output_folder, f"frame_{outframe_id}.jpg")
                cv2.imwrite(frame_filename, frame_to_save)  # Save the frame as an image

                # Append frame info to the JSON data list
                frame_info = {
                    "camera_id": self.camera_id,
                    "frame_id": outframe_id,
                    "geo_location": self.geo_location,
                    "image_path": frame_filename
                }
                self.json_data.append(frame_info)

            # Reuse the saved frame for the next 24 frames within the same second
            if frame_to_save is not None:
                cv2.imshow("Frame", frame_to_save)

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
    config_filename = "config.json"
    json_filename = "frame_info.json"
    batch_filename = "batch_info.json"

    processor = img_stream_process(camera_id, geo_location, output_folder, config_filename)
    processor.video_capture()
    processor.frame_processing()
    processor.batch_frames()
    processor.sqlserver_db()
    processor.save_json_data(json_filename)
    processor.save_batch_info(batch_filename)


    print("Frame processing and batching complete. JSON data saved to:", json_filename)
    print("Batch information saved to:", batch_filename)
