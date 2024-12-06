# bigquery-advanced-utils

![Total Downloads](https://static.pepy.tech/badge/bigquery-advanced-utils?textLength=250)
![License](https://img.shields.io/badge/license-GNU%20GPL%20v3-blue)
![Version](https://img.shields.io/github/v/release/Alessio-Siciliano/bigquery-advanced-utils)
![PyPI](https://img.shields.io/pypi/v/bigquery-advanced-utils)
![Issues](https://img.shields.io/github/issues/Alessio-Siciliano/bigquery-advanced-utils)
![Contributors](https://img.shields.io/github/contributors/Alessio-Siciliano/bigquery-advanced-utils)
![PyLint](https://img.shields.io/github/actions/workflow/status/Alessio-Siciliano/bigquery-advanced-utils/pylint.yml?branch=main&label=PyLint&logo=python)
![Black](https://img.shields.io/github/actions/workflow/status/Alessio-Siciliano/bigquery-advanced-utils/black_formatter.yml?branch=main&label=Black&logo=python)

**BigQuery-advanced-utils** is a lightweight utility library that extends the official Google BigQuery Python client.  
It simplifies tasks like **query management**, *data processing*, and *automation*.  

Aimed at **developers** and **data scientists**, the project is open to *contributions* to improve and enhance its functionality.

## Why This Library?

I created **bigquery-advanced-utils** because I often found myself facing complex or uncommon tasks when working with BigQuery, and there was little or no support available online. Rather than spending time reinventing the wheel, I decided to create this library to help others avoid the same challenges. I hope that everyone can contribute in the same spirit, so feel free to get involved and make this library even better!

## Requirements
- Python 3.10+

## Installation 📦

### Install via pip (recommended)

Run the following command in your terminal:
```bash
pip install bigquery-advanced-utils
```
### Install in a Virtual Environment

1. Create a virtual environment:
```bash
python -m venv venv
```
2. Activate the environment and install:
```bash
source venv/bin/activate  # on macOS/Linux  
venv\Scripts\activate     # on Windows  
pip install bigquery-advanced-utils
```

## Usage Examples 🚀
### Quick Start

```python
from bigquery_advanced_utils.bigquery import BigQueryClient

# Initialize helper with your project ID
helper = BigQueryClient(project_id="your_project_id")

# Load data from CSV
helper.load_data_from_csv(file_path="your_data.csv")
```
### Data Validation
```python
test_functions=[
    partial(
        # Check if any null values exist in the "age" column
        b.check_no_nulls, columns_to_test=["age"],
    ),
    partial(
        # Ensure values in the "email" column are unique
        b.check_unique_column, columns_to_test=["email"]
    )
]
```
### Search by table or owner in Datatransfer
```python
from bigquery_advanced_utils.datatransfer import DataTransferClient

helper = DataTransferClient()
# Call the function with two parameters: owner email and project id
list_transfer_config_by_owner_email(owner_email="my-email@email.com", project_id="my-project")

# Get the scheduled queries by the name of a table (it's case sensitive) 
list_transfer_configs_by_table(table_id="my-table", project_id="my-project")
```

## Planned features 🚧
- A new query builder.
- Custom data transformation and processing functions.
- Exclusive features with datatransfer.
- Utility functions to manipulate strings and query.

## Contributing

We are always open to contributions! Whether you have a bug fix, a feature request, or a general improvement to make, your help is appreciated. Here are some ways you can contribute:

- **Bug reports**: Help us catch issues before they affect users.
- **New features**: Suggest new functionalities that could improve the usability of the package.
- **Code improvements**: Review the code and suggest optimizations or fixes.

Please follow the [contributing guide](CONTRIBUTING.md) for more details on how to get started.

## License
This project is licensed under the **GNU General Public License**. See the [LICENSE](LICENSE) file for details.


## Contact
For questions or feedback, feel free to open an issue or reach out to me.
