import pandas as pd

df = pd.read_excel("lexique3/Lexique383.xlsb")

df[["1_ortho", "3_lemme", "4_cgram"]].to_csv("lexique.csv", sep=',', index=False)
