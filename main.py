from src.configuration.ReadFile import ReadConfig
from src.memory_management.GetFrame import FrameCollector
from src.cleaning_pipeline.DataPreProcessing import DataCleaning

from pathlib import Path

path= Path(__file__).resolve().parent/'config'/'config.yaml'
config= ReadConfig(archivo=path).read_config()
frame= FrameCollector(model=config).get_frame()
cleaning= DataCleaning(frame=frame, model=config)
resultado= cleaning.pipeline_data_cleaning(columas_representativas=['platform', 'genre'])

