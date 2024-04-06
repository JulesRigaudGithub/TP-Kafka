curl -o lexique3.zip http://www.lexique.org/databases/Lexique383/Lexique383.zip
unzip lexique3.zip -d lexique3

python build_csv.py

rm -r lexique3
rm lexique3.zip
