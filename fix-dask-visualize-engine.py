import dask
import dask.array as da

# Ensure the visualization engine is set correctly
dask.config.set({"visualization.engine": "cytoscape"})
print("Current visualization engine:", dask.config.get("visualization.engine"))

# Verify ipycytoscape is installed
try:
    import ipycytoscape
    print("ipycytoscape is installed correctly!")
except ImportError:
    print("Error: ipycytoscape is not installed. Please install it using:")
    print("pip install ipycytoscape OR conda install -c conda-forge ipycytoscape")

# Create a test Dask array and visualize it
x = da.ones((15, 15), chunks=(5, 5))
y = x + x.T
y.visualize()
