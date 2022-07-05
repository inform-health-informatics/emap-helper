Running notes

2022-01-28
----------
forked from emap-helper
manually copied over excluded folders and files
github actions not working
set up branch protection

```sh
cd hyschool
conda activate jupyter-book
cd book
jupyter-book clean emap
jupyter-book build emap
```

2022-01-29
----------
moving hylode vignettes into one place
see 20-flowehr
then rebuilt the book
had to install xeus-python via conda
and got confused and did a whole python upgrade in the jupyter-book environment from 3.8 to 3.9; probably was not necessary

2022-01-31
----------
moved old plotly dash vignettes in
locally `pip install jupyter_dash`
then did not work
uninstalled and tried conda for jupyter-dash 
would not install 
back to pip as per 
https://www.npmjs.com/package/jupyterlab-dash
but this time with a build step

still not working
created a clean conda environment
then installed
https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html

```
conda create --name jupyterdash python=3.9
conda activate jupyterdash
conda install pandas numpy 
conda install -c conda-forge jupyterlab
```
conda install does not work, but pip install does
https://medium.com/plotly/introducing-jupyterdash-811f1f57c02e

giving up for tonight
keep getting the following error even with a MWe
finally!

https://stackoverflow.com/questions/70908709/jupyterdash-app-run-server-error-using-jupyter-notebook/70918820

so a problem with dash 2.1
downgraded to 2.0.0

so now dash-example works