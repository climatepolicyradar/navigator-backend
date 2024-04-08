# Tests

## Local development

```shell
docker-compose run backend pytest
```

Any arguments to pytest can also be passed after this command.

It is also possible to run the backend tests via make:

```shell
make test_backend
```

## Common errors

`TypeError: Expected a string value` could mean that you're missing an
environment variable in your `.env`.

See `.env.example` for a comprehensive list of variables.
