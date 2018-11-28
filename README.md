JuliaDB TrueFX demonstration
============================

This is a demonstration of JuliaDB loading and manipulating the TrueFX tick dataset (available from https://www.truefx.com/?page=downloads)

To set up, do:
```
export JULIA_PROJECT=$(pwd)
julia -e 'using Pkg; pkg"instantiate"; pkg"precompile"'
jupyter notebook
```

The CSV data should be in the directory `indata`, and there should be another directory `bindata` which will be used to store the binary dataset (I set up symlinks to avoid duplicating data).

