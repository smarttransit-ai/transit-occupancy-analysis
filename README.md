# transit-occupancy-dashboard

if you do not have this environment then create it.

## One time setup

Check your environments.
```bash
conda env list
```
Remove as needed.
```bash
conda --yes remove --name transit_dashboard --all
```
Create and activate the new environment.
```bash
conda create --name transit_dashboard python=3.8
conda activate transit_dashboard
```

Add the packages into the new environment.
It is tempting [to use pip](
https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#using-pip-in-an-environment).
It may be that some packages are not available in the main conda repositories for your platform.
In that case you may need to add additional channels.
```bash
# conda config --prepend channels conda-forge
conda config --set channel_priority false

conda install --yes --file requirements.txt
```

## converting between basic python and notebooks

use ipynb-py-convert
```bash
conda --yes install ipynb-py-convert
```

## Running app

```bash
conda activate transit_dashboard
python app.py
```



