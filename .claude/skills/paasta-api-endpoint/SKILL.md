---
name: paasta-api-endpoint
description: Automates the creation of new PaaSTA API endpoints following established patterns
disable-model-invocation: true
allowed-tools: Read, Edit, Write, Grep, Glob, Bash, AskUserQuestion
---

# PaaSTA API Endpoint Generator

## Description
Automates the creation of new PaaSTA API endpoints following established patterns. This skill guides you through adding a new endpoint to the PaaSTA API, including view function, route registration, Swagger/OpenAPI documentation, and comprehensive tests.

## Usage
```
/paasta-api-endpoint
```

## What This Skill Does

This skill will:
1. Gather endpoint requirements interactively
2. Generate a view function in `paasta_tools/api/views/`
3. Register the route in `paasta_tools/api/api.py`
4. Add Swagger 2.0 documentation to `swagger.json`
5. Add OpenAPI 3.0 documentation to `oapi.yaml`
6. Generate unit tests following PaaSTA conventions
7. Run the OpenAPI code generator
8. Run tests to verify the implementation

## When to Use This Skill

Use this skill when you need to:
- Add a new GET/POST/PUT/DELETE endpoint to the PaaSTA API
- Ensure consistency with existing API patterns
- Automatically generate boilerplate code and documentation
- Get comprehensive test coverage from the start

## Instructions

When this skill is invoked, follow these steps:

### Step 1: Gather Requirements

Ask the user the following questions (use AskUserQuestion tool for better UX):

1. **Endpoint purpose**: What does this endpoint do? (brief description)
2. **HTTP method**: GET, POST, PUT, or DELETE?
3. **URL pattern**: What's the URL path? (e.g., `/v1/services/{service}/instances/{instance}/status`)
4. **Path parameters**: What path parameters are needed? (e.g., service, instance, deploy_group)
5. **Query parameters**: Any query parameters? (optional)
6. **Request body**: Does this endpoint accept a request body? If yes, what fields?
7. **Response structure**: What does the response look like? (e.g., `{"status": "running", "count": 5}`)
8. **Error cases**: What error scenarios should be handled? (e.g., 404 not found, 500 config error)
9. **View file**: Which view file should contain this endpoint?
   - Use existing: `service.py`, `instance.py`, `autoscaler.py`, etc.
   - Or create new: provide filename
10. **Utility functions**: What existing utility functions from `paasta_tools/utils.py` will be used?

### Step 2: Generate View Function

Create the view function following this template:

```python
@view_config(route_name="<route_name>", request_method="<METHOD>", renderer="json")
def <function_name>(request):
    """<Docstring describing what this endpoint does>."""
    # Extract parameters from request
    param1 = request.swagger_data.get("param1")
    param2 = request.swagger_data.get("param2")
    soa_dir = settings.soa_dir

    try:
        # Call utility functions to get data
        result = some_utility_function(param1, param2, soa_dir=soa_dir)

        # Build response
        response_body = {"key": result}
        return Response(json_body=response_body, status_code=200)

    except SpecificException as e:
        raise ApiFailure(str(e), 404)

    except AnotherException as e:
        raise ApiFailure(str(e), 500)
```

**Important conventions:**
- Import `Response` from `pyramid.response` for explicit status codes
- Import `ApiFailure` from `paasta_tools.api.views.exception` for error handling
- All imports at the top of the file (no inline imports)
- Use `settings.soa_dir` to get the SOA configuration directory
- Return 200 for success, 404 for not found, 500 for server errors
- Always include proper error handling with try/except

### Step 3: Register Route

Add route registration to `paasta_tools/api/api.py`:

```python
config.add_route(
    "<route_name>",
    "<url_pattern>",
)
```

Find the appropriate location (routes are loosely grouped by functionality).

### Step 4: Add Swagger 2.0 Documentation

Update `paasta_tools/api/api_docs/swagger.json`:

1. Add endpoint definition to `"paths"` section:
```json
"/path/{param}": {
    "get": {
        "responses": {
            "200": {
                "description": "Success description",
                "schema": {
                    "$ref": "#/definitions/ResponseSchema"
                }
            },
            "404": {
                "description": "Not found description"
            }
        },
        "summary": "Brief summary",
        "operationId": "operation_id",
        "tags": ["service"],
        "parameters": [
            {
                "in": "path",
                "description": "Parameter description",
                "name": "param",
                "required": true,
                "type": "string"
            }
        ]
    }
}
```

2. Add response schema to `"definitions"` section if needed:
```json
"ResponseSchema": {
    "description": "Schema description",
    "type": "object",
    "properties": {
        "field": {
            "type": "string",
            "description": "Field description"
        }
    },
    "required": ["field"]
}
```

**Nullable fields in Swagger 2.0:**
For fields that can be `null`, use the `x-nullable` extension:
```json
"optional_field": {
    "type": "string",
    "description": "This field can be null",
    "x-nullable": true
}
```

### Step 5: Add OpenAPI 3.0 Documentation

Update `paasta_tools/api/api_docs/oapi.yaml`:

1. Add schema to `components/schemas` section:
```yaml
ResponseSchema:
  description: Schema description
  type: object
  properties:
    field:
      type: string
      description: Field description
  required:
    - field
```

2. Add endpoint to `paths` section:
```yaml
/path/{param}:
  get:
    operationId: operation_id
    parameters:
    - description: Parameter description
      in: path
      name: param
      required: true
      schema:
        type: string
    responses:
      "200":
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ResponseSchema'
        description: Success description
      "404":
        description: Not found description
    summary: Brief summary
    tags:
    - service
```

**Nullable fields in OpenAPI 3.0:**
For fields that can be `null`, use the `nullable` property:
```yaml
optional_field:
  type: string
  description: This field can be null
  nullable: true
```

### Step 6: Generate Unit Tests

Create comprehensive unit tests in `tests/api/test_<view_file>.py`:

**Test conventions:**
- Use context manager form of mocking (`with mock.patch(...) as mock_name:`)
- Always use `autospec=True` for patches
- Use `spec=ClassName` for Mock objects that represent class instances
- Test success case (200 response)
- Test all error cases (404, 500, etc.)
- Use descriptive test names: `test_<function_name>_<scenario>`
- Use descriptive docstrings

**Test template:**
```python
def test_endpoint_name_success():
    """Test successful response."""
    with mock.patch(
        "paasta_tools.api.views.<module>.<function>", autospec=True
    ) as mock_function:
        mock_function.return_value = "expected_value"

        request = testing.DummyRequest()
        request.swagger_data = {"param": "value"}

        response = endpoint_function(request)
        assert response.status_code == 200
        assert response.json_body == {"key": "expected_value"}


def test_endpoint_name_not_found():
    """Test 404 when resource not found."""
    with mock.patch(
        "paasta_tools.api.views.<module>.<function>", autospec=True
    ) as mock_function:
        mock_function.side_effect = SomeException("not found")

        request = testing.DummyRequest()
        request.swagger_data = {"param": "value"}

        with pytest.raises(ApiFailure) as exc_info:
            endpoint_function(request)
        assert exc_info.value.msg == "not found"
        assert exc_info.value.err == 404
```

### Step 7: Generate OpenAPI Client Code

Run the code generator:
```bash
make openapi-codegen
```

This regenerates the Python client code in `paasta_tools/paastaapi/` based on the updated `oapi.yaml`.

### Step 8: Run Tests and Validation

1. Run the new tests:
```bash
.tox/py310-linux/bin/pytest tests/api/test_<view_file>.py::<test_name> -xvs
```

2. Run mypy type checking:
```bash
.tox/py310-linux/bin/mypy paasta_tools/api/views/<view_file>.py
.tox/py310-linux/bin/mypy tests/api/test_<view_file>.py
```

3. Run pre-commit checks:
```bash
.tox/py310-linux/bin/pre-commit run --files paasta_tools/api/views/<view_file>.py tests/api/test_<view_file>.py paasta_tools/api/api.py paasta_tools/api/api_docs/swagger.json paasta_tools/api/api_docs/oapi.yaml
```

4. Stage the generated files:
```bash
git add paasta_tools/paastaapi/
```

### Step 9: Summary

Provide a summary of:
- Files created/modified
- Endpoint URL and method
- Test coverage (number of tests added)
- Any manual steps needed

## Example Reference

See the container image endpoint implementation as a reference:
- View: `paasta_tools/api/views/service.py:43-63`
- Route: `paasta_tools/api/api.py:166-169`
- Tests: `tests/api/test_service.py:63-143`

## Common Patterns

### Error Handling
- `NoDeploymentsAvailable` → 404
- `KeyError` for missing config fields → 500
- `ValueError` for invalid input → 400
- Generic exceptions → 500

### Response Patterns
- Single value: `{"field_name": "value"}`
- List: `{"items": [...]}`
- Complex object: Use TypedDict schema

### Utility Functions
Common utilities are usually found in `paasta_tools/utils.py`

## Notes

- **Atomic commits**: Each endpoint should be a single, self-contained commit
- **Bisectable history**: All tests must pass after adding the endpoint
- **Type safety**: Use type hints and ensure mypy passes
- **Documentation**: Keep Swagger and OpenAPI docs in sync
- **Testing**: Aim for 100% coverage of the new endpoint

## Skill Exit

After completing all steps successfully, provide the user with:
1. Summary of changes
2. Test results
3. Verification that pre-commit checks pass
4. Suggested commit message following PaaSTA conventions
