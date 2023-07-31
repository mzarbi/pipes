import json
import os
import glob
import shutil
import fnmatch
import re
from abc import ABC, abstractmethod

from pipeline import TransformationStep


class FileCopyStep(TransformationStep):
    """
    FileCopyStep is a TransformationStep in the pipeline which copies files from a source directory to a
    destination directory based on provided configuration.

    The source directory is read from the pipeline configuration.
    The destination directory is created inside the pipeline's playground.

    The behavior of the copy operation can be customized with the following step configuration options:
    - 'deep_search': If true, files are searched recursively in subdirectories. Default is False.
    - 'pattern': A pattern that filenames should match. The interpretation depends on 'pattern_type'.
    - 'pattern_type': Can be 'unix', 'regex', 'startswith', or 'endswith'. Determines how 'pattern' is interpreted.
    """

    def run(self):
        """
        Runs the file copy operation. Files are copied from source directory to destination directory
        based on the step configuration options.

        If 'deep_search' is true, the search for files is done recursively and the directory structure
        is preserved in the destination directory.

        The 'pattern' and 'pattern_type' options determine which files are copied. The filename is matched against
        the 'pattern' according to the 'pattern_type':
        - 'unix': File name is matched using Unix shell-style wildcards.
        - 'regex': File name is matched using regular expressions.
        - 'startswith': File name is matched if it starts with the pattern string.
        - 'endswith': File name is matched if it ends with the pattern string.

        After all files are copied, the next step in the pipeline is returned.

        Returns:
            str: The name of the next step to be executed in the pipeline.
        """
        # Extract directories and parameters from the pipeline configuration
        source_dir = self.pipeline.cfg['cft_folder_path']
        dest_dir = os.path.join(self.pipeline.playground_path, self.pipeline.cfg['temp_folder_name'])
        deep_search = self.step_cfg.get('deep_search', False)
        pattern = self.step_cfg.get('pattern', '*')
        pattern_type = self.step_cfg.get('pattern_type', 'unix')  # unix, regex, startswith, endswith

        # Create the destination directory if it doesn't exist
        os.makedirs(dest_dir, exist_ok=True)

        # Pattern matching function based on the pattern type
        def matches_pattern(filename):
            if pattern_type == 'unix':
                return fnmatch.fnmatch(filename, pattern)
            elif pattern_type == 'regex':
                return re.match(pattern, filename) is not None
            elif pattern_type == 'startswith':
                return filename.startswith(pattern)
            elif pattern_type == 'endswith':
                return filename.endswith(pattern)
            else:
                raise ValueError(f"Unknown pattern type: {pattern_type}")

        if deep_search:
            # Recursive search using os.walk
            for root, dirs, files in os.walk(source_dir):
                for filename in filter(matches_pattern, files):
                    source_file = os.path.join(root, filename)
                    # Preserve the folder structure when copying
                    rel_path = os.path.relpath(source_file, source_dir)
                    dest_file = os.path.join(dest_dir, rel_path)
                    os.makedirs(os.path.dirname(dest_file), exist_ok=True)

                    shutil.copy(source_file, dest_file)
                    self.info(f"Copied file {source_file} to {dest_file}")
        else:
            # Non-recursive search using os.listdir
            for filename in filter(matches_pattern, os.listdir(source_dir)):
                source_file = os.path.join(source_dir, filename)
                dest_file = os.path.join(dest_dir, filename)

                shutil.copy(source_file, dest_file)
                self.info(f"Copied file {source_file} to {dest_file}")

        return 'FileValidationStep'


class FileValidationError(Exception):
    """Exception raised for errors during file validation."""

    def __init__(self, filepath, message):
        self.filepath = filepath
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'FileValidationError for {self.filepath}: {self.message}'


class FileValidationStep(TransformationStep):
    def run(self):
        # Get the temp directory path
        temp_dir = os.path.join(self.pipeline.playground_path, self.pipeline.cfg['temp_folder_name'])

        # Read error handling strategy from step config, default to 'abort'
        error_strategy = self.step_cfg.get('error_strategy', 'abort')

        # List to store invalid files info
        invalid_files = []

        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                try:
                    # Validate each file
                    file_path = os.path.join(root, file)
                    if not self.validate_file_type(file_path):
                        raise FileValidationError(file_path, "Invalid file type")
                    if not self.validate_file_contents(file_path):
                        raise FileValidationError(file_path, "Invalid file content")
                except Exception as e:
                    # Log the error
                    self.error(f"Error validating file {file_path}: {str(e)}")

                    # Store the invalid file info
                    invalid_files.append({'file': file_path, 'error': str(e)})

                    if error_strategy == 'abort':
                        # Abort the pipeline
                        raise e
                    elif error_strategy == 'delete':
                        # Delete the file
                        os.remove(file_path)
                        self.info(f"Deleted invalid file {file_path}")

        # Write invalid files info to json
        with open(os.path.join(self.pipeline.playground_path, 'invalid.json'), 'w') as f:
            json.dump(invalid_files, f)

        if error_strategy != 'ignore' and len(invalid_files) > 0:
            # If the strategy is not 'ignore' and there are invalid files, abort the pipeline
            raise ValueError('Invalid files found, check invalid.json for details')

        # After all files have been processed, move to the next step
        return 'PreprocessingStep'

    def validate_file_type(self, file_path):
        # Implement file type validation logic here
        return True

    def validate_file_contents(self, file_path):
        # Implement file content validation logic here
        return True


class PreprocessingStep(TransformationStep, ABC):
    """
    A pipeline step for preprocessing files.

    This step performs a series of transformations on the data in the files.
    The specific transformations are configurable and may include tasks such as
    data cleaning, normalization, and encoding. The processed files are stored in a
    staging temporary folder specified in the configuration.
    """

    def run(self):
        # Extract parameters from the step configuration
        staging_temp_folder = self.pipeline.get('staging_temp_folder', 'staging')
        error_handling_strategy = self.step_cfg.get('error_strategy', 'abort')

        # Create staging temp directory
        staging_dir = os.path.join(self.pipeline.playground_path, staging_temp_folder)
        os.makedirs(staging_dir, exist_ok=True)

        # Apply transformations to each file
        files_dir = os.path.join(self.pipeline.playground_path, self.pipeline.cfg['temp_folder_name'])
        for filename in os.listdir(files_dir):
            source_file = os.path.join(files_dir, filename)
            destination_file = os.path.join(staging_dir, filename)

            try:
                with self.transformation(f"Preprocessing {filename}"):
                    self.apply_preprocessing(source_file, destination_file)

            except Exception as e:
                self.error(f"Failed to preprocess {filename}: {e}")
                if error_handling_strategy == 'abort':
                    raise
                elif error_handling_strategy == 'ignore':
                    continue
                elif error_handling_strategy == 'delete':
                    os.remove(source_file)
                    with open("invalid.json", "a") as f:
                        f.write(f"{filename}: {str(e)}\n")

        return 'DataStorageStep'

    @abstractmethod
    def apply_preprocessing(self, source_file, output_file):
        raise NotImplemented


class DataStorageStep(TransformationStep):
    """
    A pipeline step for storing preprocessed data in an S3 bucket.

    This step moves the preprocessed data from the staging temporary folder to an S3 bucket.
    If an error occurs during the process, it will be logged and handled according to
    the specified error handling strategy.
    """
    def run(self):
        # Extract parameters from the step configuration
        bucket_name = self.step_cfg.get('s3_bucket')
        s3_folder = self.step_cfg.get('s3_folder')
        error_handling_strategy = self.step_cfg.get('error_handling_strategy', 'abort')

        # Create an S3 client
        s3 = boto3.client('s3')

        # Define a function to upload a file to S3
        def upload_to_s3(file_path, s3_path):
            with self.transformation(f"Storing {file_path} in {s3_path}"):
                s3.upload_file(file_path, bucket_name, s3_path)

        # Move files from staging directory to S3
        staging_dir = os.path.join(self.pipeline.playground_path, 'staging')
        for filename in os.listdir(staging_dir):
            source_file = os.path.join(staging_dir, filename)
            s3_path = f"{s3_folder}/{filename}"

            try:
                upload_to_s3(source_file, s3_path)
            except Exception as e:
                self.error(f"Failed to store {source_file} in {s3_path}: {e}")
                if error_handling_strategy == 'abort':
                    raise
                elif error_handling_strategy == 'ignore':
                    continue
                elif error_handling_strategy == 'delete':
                    os.remove(source_file)
                    with open("invalid.json", "a") as f:
                        f.write(f"{source_file}: {str(e)}\n")

        return 'NextStep'

pipeline_cfg = {
    'playground_path': './pipeline_playground',
    'steps': [
        {
            'class_name': 'FileCopyStep',
            'temp_folder_name': 'temp',
            'staging_temp_folder': 'staging',
            'deep_search': True,  # Use False for non-recursive search
            'pattern': '*.txt',  # Use '*' for all files, or specific regex pattern
            'pattern_type': 'unix',  # 'unix', 'regex', 'startswith', 'endswith'
            'retry': {
                'attempts': 3,
                'backoff': 1
            }
        },
        {
            'class_name': 'FileValidationStep',
            'retry': {
                'attempts': 3,
                'backoff': 1
            },
            'step_cfg': {
                'error_strategy': 'abort'  # 'abort', 'ignore', 'delete'
            }
        },
        {
            'class_name': 'PreprocessingStep',
            'retry': {
                'attempts': 3,
                'backoff': 1
            },
            'step_cfg': {

                'error_strategy': 'abort',  # 'abort', 'ignore', 'delete'
            }
        },
        {
            'class_name': 'DataStorageStep',
            'retry': {
                'attempts': 3,
                'backoff': 1
            },
            'step_cfg': {
                's3_bucket': 'my-bucket',
                's3_folder': 'my-folder',
                'error_handling_strategy': 'abort',  # 'abort', 'ignore', 'delete'
            }
        }
    ],
    'initialize_step': 'FileCopyStep',
    'notifications': {
        'email_on_failure': True,
        'email_on_start': True,
        'email_on_success': True
    }
}
