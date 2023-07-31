import asyncio

from pipeline import TransformationStep, GenericPipeline



playground = r"C:\Users\medzi\Desktop\bnp\pipel\cache"
pipeline_cfg = {
    'playground_path': './pipeline_playground',
    'steps': [
        {
            'class_name': 'DataExtraction',
            'retry': {
                'attempts': 3,
                'backoff': 1
            }
        },
        {
            'class_name': 'DataTransformation',
            'retry': {
                'attempts': 3,
                'backoff': 1
            }
        },
        {
            'class_name': 'xxx',
            'retry': {
                'attempts': 3,
                'backoff': 1
            }
        }
    ],
    'initialize_step': 'DataExtraction',
    'notifications': {
        'email_on_failure': True,
        'email_on_start': True,
        'email_on_success': True
    }
}
#pipeline = GenericPipeline(playground, pipeline_cfg)
pipeline = GenericPipeline.from_id(playground, "70b019c51b314e0f94197a79baa32148")



class DataExtraction(TransformationStep):
    def run(self):
        with self.transformation('Data extraction from database'):
            # Simulate data extraction
            self.pipeline.state['data'] = 'extracted_data'

        with self.transformation('Data extraction from database2'):
            # Simulate data extraction
            self.pipeline.state['data'] = 'extracted_data'
        next_step = 'DataTransformation'
        print("sssssssssssssssssssss")
        return next_step


class DataTransformation(TransformationStep):
    def run(self):
        with self.transformation('Data transformation'):
            # Simulate data transformation
            self.pipeline.state['data'] = 'transformed_data'
            print("qddqsdqsdqsdqsd")
            return "xxx"


@pipeline.step_registry.step
def xxx(pipeline, step_cfg):
    print(step_cfg)
    print("ssssssssssssss")



pipeline.register(DataExtraction)
pipeline.register(DataTransformation)
asyncio.run(pipeline.run())



