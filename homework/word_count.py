"""Taller evaluable"""

import os
import glob
import pandas as pd


def load_input(input_directory):
    #Lea los archivos de texto en la carpeta Input/ y almacene el contenido en un DataFrame de Pandas.
    #Cada linea del archivo debe ser una entrada en el dataframe

    # Access all the files on a given folder
    files          = glob.glob(F"{input_directory}/*")
    # Stores content files as separate items separated by file
    dataframes     = [pd.read_csv(file, header=None, delimiter="\t", names=["line"], index_col=None) for file in files] 
    # Joins all the dataframes together to make a single dataframe that has all the values appended one after another
    jointDataframe = pd.concat(dataframes, ignore_index=True)

    return jointDataframe

def line_preprocessing(sequence: pd):
    dataframeCopy = sequence.copy() #Copy to preserve original data
    dataframeCopy["line"] = dataframeCopy["line"].str.lower() # dataframeCopy.iloc[:,0] equivalent to access a column via index
    dataframeCopy["line"] = dataframeCopy["line"].str.replace(",","")
    dataframeCopy["line"] = dataframeCopy["line"].str.replace(".","")
    
    return dataframeCopy    

def wordCount(sequence: pd):
    # Mapper
    dataframeCopyCopy = sequence
    dataframeCopyCopy["line"] = dataframeCopyCopy["line"].str.split()
    dataframeCopyCopy = dataframeCopyCopy.explode("line") # Interchanges rows and columns, so we get all words in 1 column one by one
    dataframeCopyCopy = dataframeCopyCopy.groupby("line").size().reset_index(name = "count") #groupby groups, size counts
    
    return dataframeCopyCopy


def save_output(output_directory, sequence: pd):
    """Save Output"""
    if os.path.exists(output_directory):
        files = glob.glob(f"{output_directory}/*")
        for file in files:
            os.remove(file)        # deletes file one by one on a given folder 
        os.rmdir(output_directory) # deletes a given folder 
    
    os.makedirs(output_directory)  # Creates a given folder on the same address
    sequence.to_csv(f"{output_directory}/part-00000",sep='\t',index=False,header=False)


def create_marker(output_directory):
    """Create Marker"""
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")

#
# Escriba la función job, la cual orquesta las funciones anteriores.
#
def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory)
    sequence = line_preprocessing(sequence)
    sequence = wordCount(sequence)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":
    
    run_job(
        "files/input",
        "files/output",
    )
