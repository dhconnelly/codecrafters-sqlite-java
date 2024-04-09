my Java solution to the CodeCrafters
["Build Your Own SQLite" Challenge](https://codecrafters.io/challenges/sqlite)

status: [complete](https://app.codecrafters.io/users/dhconnelly) 🎉

prerequisites:

- jdk (e.g. `sdk install java` via [sdkman](https://sdkman.io/))
- maven (e.g. `sdk install maven`)

to run tests:

```
mvn test
```

some example commands:

```bash
   ./your_sqlite3.sh sample.db .dbinfo
   ./your_sqlite3.sh sample.db "SELECT count(*) FROM oranges"
   ./your_sqlite3.sh sample.db "SELECT name FROM apples"
   ./your_sqlite3.sh superheroes.db "select name, first_appearance from superheroes where hair_color = 'Brown Hair'"
   ./your_sqlite3.sh superheroes.db "select count(*) from superheroes where eye_color = 'Blue Eyes'"
   ./your_sqlite3.sh companies.db "SELECT id, name FROM companies WHERE country = 'republic of the congo'"
```
