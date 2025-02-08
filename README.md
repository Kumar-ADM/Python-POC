This is the sample code to prossess raw data to refined data process using python.

Install the chocolety in the windows powershell
Then, Install go-task tool to automate the process.

1. Install Taskfile Using Chocolatey
Now, run:

powershell
choco install go-task

2.Once installed, verify the installation with:

powershell
task --version


Project Structure:
my_project/
│── load_csv_to_postgres/          # Python package
│   │── __init__.py                # Makes this directory a package
│   │── load_csv_to_postgres.py    # Your main script
|___|__customers_order_join/
|   |__customers_order_join.py
|__ |business_data
|__ |__business_data.py
│── tests/
│   │── test_load_csv_to_postgres.py
│── setup.py
│── requirements.txt
│── Taskfile.yml

1️⃣ Setup Environment
CMD
task setup
2️⃣ Build the Package
CMD
task build
3️⃣ Install the Package Locally
CMD
task install
4️⃣ Run the CSV Loader
CMD
task run
5️⃣ Run Unit Tests
CMD
task test
6️⃣ Clean Up Build Files
CMD
task clean
