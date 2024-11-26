import pandas as pd
import numpy as np
from datetime import datetime

crimes = pd.read_csv("./data/crime.csv", encoding='unicode_escape')

##Basic operations to inspect data
crimes.shape #(319073, 17)

crimes['SHOOTING'].unique() #[nan 'Y']

crimes.isna().any()  #DISTRICT, SHOOTING, UCR_PART, STREET, Lat, Long columnas que tienen null
print(crimes.isnull().sum())  #
