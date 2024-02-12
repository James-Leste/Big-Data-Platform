import pandas as pd
import numpy as np
import json
FILEPATH1 = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/amazon_reviews_us_Digital_Software_v1_00.tsv"
FILEPATH2 = "/Users/jamesroot/AAAroot/Course notes/Aalto Big Data Platforms/assignment-1-101699682/test/amazon_reviews_us_Gift_Card_v1_00.tsv"
def dataInit(filePath):
    df = pd.read_csv(filepath_or_buffer=filePath, sep="\t", )
    print(df.info())
    x = df.head().to_json()
    y = json.loads(x)
    return y



def main():
    j = dataInit(FILEPATH2)
    print(j)
    

main()