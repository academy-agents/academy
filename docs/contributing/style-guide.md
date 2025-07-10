The Python code and docstring format mostly follows Google's [Python Style Guide](https://google.github.io/styleguide/pyguide.html){target=_blank}, but the pre-commit config is the authoritative source for code format compliance.

**Nits:**

* Avoid redundant comments---write _why_ and not _what_.
* Keep comments and docstrings up-to-date when changing functions and classes.
* Don't include unrelated formatting or refactors in a feature PR.
* Avoid imports in `__init__.py` (reduces the likelihood of circular imports).
* Prefer pure functions where possible.
* Define all class attributes inside `__init__` so all attributes are visible in one place.
  Attributes that are defined later can be set as `None` as a placeholder.
* Prefer f-strings (`#!python f'name: {name}`) over string format (`#!python 'name: {}'.format(name)`).
  Never use the `%` operator.
* Prefer [typing.NamedTuple][] over [collections.namedtuple][].
* Use sentence case for error and log messages, but only include punctuation for errors.
  ```python
  logger.info(f'new connection opened to {address}')
  raise ValueError('Name must contain alphanumeric characters only.')
  ```
* Document all exceptions that may be raised by a function in the docstring.
