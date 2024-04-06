import pandas as pd

df = pd.read_excel("lexique3/Lexique383.xlsb")

df_clean = df[["1_ortho", "3_lemme", "4_cgram"]].dropna()
df_clean.to_csv("lexique.csv", sep=',', index=False, header=False)
