# Building Data Applications with Go Workshop

This is a workshop about building data applications with Go. 

## Prerequisites
Install 
- brew (if using macos): https://brew.sh/
- go: https://golang.org/doc/install
- docker and docker-compose: https://docs.docker.com/get-docker/
- git: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git

Initialize go module:
```
go mod init
```

Install `sqlc` as a global command line utility:

```
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
``` 

Note that sqlc is not a runtime dependency, it's just a tool that helps you to generate boilerplate code from SQL queries.

Start postgres:

```
docker-compose up
```

# Creating migrations
Install the CLI tool. Again, no runtime dependency, just a tool for your dev environment:

```
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

Then, create a migration (optional):

```
migrate create -ext sql -dir ./adapters/db/scripts/migrations new_migration.sql
```

And run the migration:

```
migrate -database sqlite3://test.db -path adapters/db/scripts/migrations up
```