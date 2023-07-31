import datetime
import json
import logging
import os
import pickle
import uuid
from functools import update_wrapper
from typing import List, Callable, Any

import asyncio

from filelock import FileLock


class StepRegistry:
    """Registry for storing and accessing step classes."""

    def __init__(self):
        self._registry = {}

    def register(self, step_class):
        """Register a step class in the registry.

        Parameters:
            step_class (type): The step class to be registered.
        """
        self._registry[step_class.__name__] = step_class

    def get(self, class_name):
        """Retrieve a step class from the registry by its name.

        Parameters:
            class_name (str): The name of the step class.

        Returns:
            type: The step class if found, otherwise None.
        """
        return self._registry.get(class_name)

    def step(self, f=None, **kwargs):
        """Decorator to convert a function into a step class.

        Parameters:
            f (Callable): The function to be converted into a step class.

        Returns:
            Callable: The step class.
        """
        def decorator(func: Callable) -> Callable:
            class FunctionStep(BaseStep):
                def __init__(self, pipeline, step_cfg):
                    super().__init__(pipeline, step_cfg)

                def run(self):
                    return func(self.pipeline, self.step_cfg)

            FunctionStep.run = update_wrapper(FunctionStep.run, func)
            FunctionStep.__name__ = func.__name__
            self.register(FunctionStep)

            return FunctionStep

        if f is None:
            return decorator
        else:
            return decorator(f)


class BaseStep:
    """Base class for defining pipeline steps."""

    def __init__(self, pipeline, step_cfg):
        """Initialize a pipeline step.

        Parameters:
            pipeline (BasePipeline): The pipeline this step belongs to.
            step_cfg (dict): The configuration for this step.
        """
        self.pipeline = pipeline
        self.step_cfg = step_cfg
        self.id = self.step_cfg.get('id', self.__class__.__name__)


    def logger(self):
        return self.pipeline.logger


    def info(self, msg):
        return self.logger().info(msg)

    def error(self, msg):
        return self.logger().error(msg)

    def run(self):
        """Method to be overridden by derived classes. Performs the actual step execution."""
        raise NotImplementedError

    async def run_step(self):
        """Asynchronously run the step and handle exceptions.

        Returns:
            Tuple[bool, Optional[Exception], Any]: A tuple with a boolean indicating success, an optional exception
            if an error occurred, and the result of the step execution.
        """
        try:
            return True, None, self.run()
        except Exception as e:
            return False, e, None


class BasePipeline:
    """Base class for defining pipelines."""

    def __init__(self, playground_path: str, cfg: dict = None, id: str = None):
        """Initialize a pipeline.

        Parameters:
            playground_path (str): The path to the playground where pipeline data will be stored.
            cfg (dict, optional): The configuration for the pipeline. Defaults to None.
            id (str, optional): The ID of the pipeline. Defaults to None.
        """
        self.cfg = cfg or {}
        self.id = id or self._generate_id()
        self.playground_path = os.path.join(playground_path, self.id)

        os.makedirs(self.playground_path, exist_ok=True)
        with open(f"{self.playground_path}/pipeline.json", "w") as file:
            json.dump(self.cfg, file, indent=4)

        self.logger = self._init_logging()

    @staticmethod
    def _generate_id():
        """Generate a unique ID using UUID4.

        Returns:
            str: A unique ID in hexadecimal format.
        """
        return uuid.uuid4().hex

    def _init_logging(self):
        """Initialize and configure the logger for the pipeline.

        Returns:
            logging.Logger: The configured logger instance.
        """
        logger = logging.getLogger(self.id)
        logger.setLevel(logging.INFO)

        handler = logging.FileHandler(f"{self.playground_path}/debug.log")
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        return logger

    def build_steps(self):
        """Build and return the steps for the pipeline.

        Returns:
            dict: A dictionary containing the step class names as keys and their corresponding instances as values.
        """
        raise NotImplementedError("Subclasses must implement the 'build_steps' method.")

    async def run(self):
        """Asynchronously run the pipeline, executing its steps sequentially.

        Raises:
            NotImplementedError: Subclasses must implement the 'run' method.
        """
        raise NotImplementedError("Subclasses must implement the 'run' method.")


class TransformationStep(BaseStep):
    """Step class for defining transformations within a pipeline."""

    def __init__(self, *args, **kwargs):
        """Initialize a transformation step.

        Parameters:
            *args: Positional arguments for the parent class.
            **kwargs: Keyword arguments for the parent class.
        """
        super().__init__(*args, **kwargs)
        self.transformations = []

    def transformation(self, description, *args, **kwargs):
        """Context manager to define a transformation within the step.

        Parameters:
            description (str): Description of the transformation.
            *args: Positional arguments for the transformation.
            **kwargs: Keyword arguments for the transformation.

        Returns:
            TransformationContextManager: A context manager for the transformation.
        """
        return TransformationContextManager(self, description, *args, **kwargs)

    def save_transformations(self):
        """Save the transformations performed within the step to a file."""
        os.makedirs(os.path.join(self.pipeline.playground_path, "etl"), exist_ok=True)
        etl_file = os.path.join(self.pipeline.playground_path, "etl", f"{self.id}_etl.json")
        with FileLock(f"{etl_file}.lock"):
            with open(etl_file, 'w') as f:
                json.dump(self.transformations, f)


class TransformationContextManager:
    """Context manager for handling transformations within a step."""

    def __init__(self, step, description, *args, **kwargs):
        """Initialize a transformation context manager.

        Parameters:
            step (TransformationStep): The transformation step this context manager belongs to.
            description (str): Description of the transformation.
            *args: Positional arguments for the transformation.
            **kwargs: Keyword arguments for the transformation.
        """
        self.step = step
        self.description = description
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        """Context manager entry point, called when the context is entered."""
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point, called when the context is exited."""
        self.step.transformations.append({
            'description': self.description,
            'args': self.args,
            'kwargs': self.kwargs,
            'timestamp': datetime.datetime.utcnow().isoformat()
        })
        self.step.save_transformations()


class GenericPipeline(BasePipeline):
    """A generic pipeline implementation with hooks and notifications support."""

    def __init__(self, *args, **kwargs):
        """Initialize a generic pipeline.

        Parameters:
            *args: Positional arguments for the parent class.
            **kwargs: Keyword arguments for the parent class.
        """
        super().__init__(*args, **kwargs)
        self.state = {}
        self.step_registry = StepRegistry()

        # Hooks related methods
        self.step_start_hooks: List[Callable[[BasePipeline, str], Any]] = []
        self.step_finish_hooks: List[Callable[[BasePipeline, str], Any]] = []
        self.step_failure_hooks: List[Callable[[BasePipeline, str, Exception], Any]] = []

        # If email notifications are enabled, add them to appropriate hooks
        if self.cfg.get('notifications', {}).get('email_on_failure', False):
            self.hook_step_failure(self.send_email_on_failure)
        if self.cfg.get('notifications', {}).get('email_on_start', False):
            self.hook_step_start(self.send_email_on_start)
        if self.cfg.get('notifications', {}).get('email_on_success', False):
            self.hook_step_finish(self.send_email_on_success)

    @staticmethod
    def from_id(playground_path: str, id: str):
        """Create a `GenericPipeline` instance from a specified ID.

        This method loads the configuration of a pipeline from a JSON file based on the given `playground_path` and `id`,
        and returns a `GenericPipeline` instance with the loaded configuration.

        Parameters:
            playground_path (str): The path to the playground where the pipeline data is stored.
            id (str): The ID of the pipeline.

        Returns:
            GenericPipeline: A `GenericPipeline` instance with the loaded configuration.

        Raises:
            FileNotFoundError: If the pipeline configuration file with the given ID does not exist.
            json.JSONDecodeError: If there is an error decoding the JSON configuration file.
        """
        with open(f"{playground_path}/{id}/pipeline.json", "r") as file:
            cfg = json.load(file)
        return GenericPipeline(playground_path, cfg, id)

    def register(self, step_class):
        """Register a step class in the step registry.

        Parameters:
            step_class (type): The step class to be registered.
        """
        self.step_registry.register(step_class)

    def non_serializable_attributes(self):
        """Get a list of attributes that should not be serialized when saving the pipeline state.

        Returns:
            List[str]: A list of attribute names.
        """
        return ['logger', 'step_start_hooks', 'step_finish_hooks', 'step_failure_hooks']

    def get_state(self):
        """Get the current state of the pipeline, excluding non-serializable attributes.

        Returns:
            dict: A dictionary representing the serializable state of the pipeline.
        """
        state = self.__dict__.copy()
        for attr in self.non_serializable_attributes():
            state.pop(attr, None)
        return state

    def set_state(self, state):
        """Set the state of the pipeline using a given state dictionary.

        Parameters:
            state (dict): A dictionary representing the state of the pipeline.
        """
        self.__dict__.update(state)

    def save_state(self, step_name):
        """Save the state of the pipeline to a file.

        Parameters:
            step_name (str): The name of the current step.
        """
        state_folder = os.path.join(self.cfg.get('playground_path', '.'), 'pipeline_states')
        os.makedirs(state_folder, exist_ok=True)
        state_file = os.path.join(state_folder, f'{step_name}_state.pkl')
        with open(state_file, 'wb') as f:
            pickle.dump(self.get_state(), f)

    def load_state(self, step_name):
        """Load the state of the pipeline from a file.

        Parameters:
            step_name (str): The name of the current step.
        """
        state_folder = os.path.join(self.cfg.get('playground_path', '.'), 'pipeline_states')
        state_file = os.path.join(state_folder, f'{step_name}_state.pkl')
        with open(state_file, 'rb') as f:
            loaded_state = pickle.load(f)
            self.set_state(loaded_state)

    def hook_step_start(self, hook: Callable[[BasePipeline, str], Any]):
        """Register a hook to be executed before a step starts.

        Parameters:
            hook (Callable[[BasePipeline, str], Any]): The hook function to be registered.
        """
        self.step_start_hooks.append(hook)

    def hook_step_finish(self, hook: Callable[[BasePipeline, str], Any]):
        """Register a hook to be executed after a step finishes successfully.

        Parameters:
            hook (Callable[[BasePipeline, str], Any]): The hook function to be registered.
        """
        self.step_finish_hooks.append(hook)

    def hook_step_failure(self, hook: Callable[[BasePipeline, str, Exception], Any]):
        """Register a hook to be executed if a step fails with an exception.

        Parameters:
            hook (Callable[[BasePipeline, str, Exception], Any]): The hook function to be registered.
        """
        self.step_failure_hooks.append(hook)

    def build_steps(self):
        """Build and return the steps for the pipeline.

        Returns:
            dict: A dictionary containing the step class names as keys and their corresponding instances as values.
        """
        steps_cfg = self.cfg.get('steps', [])
        steps = {}
        for step_cfg in steps_cfg:
            step_class_name = step_cfg.get('class_name')
            step_class = self.step_registry.get(step_class_name)
            step = step_class(self, step_cfg)
            steps[step_class_name] = step
        return steps

    async def run(self):
        """Asynchronously run the pipeline, executing its steps sequentially.

        Raises:
            NotImplementedError: Subclasses must implement the 'run' method.
        """
        self.steps = self.build_steps()
        current_step_name = self.cfg.get('initialize_step')
        while current_step_name:
            step = self.steps.get(current_step_name)
            if step is None:
                self.logger.error(f"Step {current_step_name} not found")
                break

            for hook in self.step_start_hooks:
                hook(self, current_step_name)

            success, exception, current_step_name = await self.run_step_with_retry(step)

            if success:
                for hook in self.step_finish_hooks:
                    hook(self, current_step_name)
            else:
                for hook in self.step_failure_hooks:
                    hook(self, current_step_name, exception)

    async def run_step_with_retry(self, step):
        """Asynchronously run a step with retry logic.

        Parameters:
            step (BaseStep): The step to be executed.

        Returns:
            Tuple[bool, Optional[Exception], str]: A tuple with a boolean indicating success, an optional exception
            if an error occurred, and the name of the next step to execute.
        """
        attempts = step.step_cfg.get('retry', {}).get('attempts', 1)
        backoff = step.step_cfg.get('retry', {}).get('backoff', 0)

        for attempt in range(attempts):
            success, exception, current_step_name = await step.run_step()
            if success:
                return success, None, current_step_name

            await asyncio.sleep(backoff)

        return False, exception, current_step_name

    def send_email_on_failure(self, pipeline, step_name, exception):
        """Send an email notification on pipeline step failure.

        Parameters:
            pipeline (BasePipeline): The pipeline object.
            step_name (str): The name of the step that failed.
            exception (Exception): The exception raised during the step execution.
        """
        subject = "Pipeline execution failed"
        body = f"The pipeline {pipeline.id} failed on step {step_name} with error {str(exception)}"
        # Fill this function with code to send email notifications

    def send_email_on_start(self, pipeline, step_name):
        """Send an email notification when the pipeline starts a step.

        Parameters:
            pipeline (BasePipeline): The pipeline object.
            step_name (str): The name of the step that is starting.
        """
        subject = "Pipeline execution started"
        body = f"The pipeline {pipeline.id} started on step {step_name}."
        # Fill this function with code to send email notifications

    def send_email_on_success(self, pipeline, step_name):
        """Send an email notification on successful pipeline step completion.

        Parameters:
            pipeline (BasePipeline): The pipeline object.
            step_name (str): The name of the step that completed successfully.
        """
        subject = "Pipeline execution successful"
        body = f"The pipeline {pipeline.id} completed successfully on step {step_name}."
        # Fill this function with code to send email notifications


class ParallelStep(TransformationStep):
    """Step class for parallel processing of elements."""

    def __init__(self, pipeline, step_cfg):
        """Initialize a parallel step.

        Parameters:
            pipeline (BasePipeline): The pipeline this step belongs to.
            step_cfg (dict): The configuration for this step.
        """
        super().__init__(pipeline, step_cfg)
        self.exception_strategy = self.step_cfg.get('exception_strategy', 'continue')  # default to 'continue'

    def iterator(self) -> List[Any]:
        """Override this method to return an iterator with the elements to be processed.

        Returns:
            List[Any]: A list of elements to be processed in parallel.
        """
        return []

    async def run_on_element(self, element: Any):
        """Override this method to define what should happen on each element during parallel processing.

        Parameters:
            element (Any): An element from the iterator to be processed.
        """
        pass

    async def run(self):
        """Asynchronously run the parallel step, processing elements in parallel.

        The number of parallel workers is controlled by the 'num_workers' parameter in the step configuration.

        Raises:
            Exception: If an element processing fails and the 'exception_strategy' is set to 'abort'.
        """
        elements = self.iterator()
        num_workers = self.step_cfg.get('num_workers', 1)

        if num_workers == 1:
            for element in elements:
                try:
                    await self.run_on_element(element)
                except Exception as e:
                    if self.exception_strategy == 'continue':
                        self.logger.error(f'Error processing element {element}: {e}')
                    elif self.exception_strategy == 'abort':
                        raise
        else:
            sem = asyncio.Semaphore(num_workers)

            async def worker(element):
                async with sem:
                    try:
                        await self.run_on_element(element)
                    except Exception as e:
                        if self.exception_strategy == 'continue':
                            self.logger.error(f'Error processing element {element}: {e}')
                        elif self.exception_strategy == 'abort':
                            raise

            await asyncio.gather(*[worker(element) for element in elements])
