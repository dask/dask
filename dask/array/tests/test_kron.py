#test_kron.py
import numpy as np
import dask.array as da
import pytest
from dask.array.kron import kron
from dask.array.utils import assert_eq

def test_kron_1d():
    """
    Test for 1D Kronecker product.
    Prueba del producto de Kronecker 1D.
    """
    # Test del caso unidimensional:
    # Calcula el producto de Kronecker entre dos vectores 1D
    x_np = np.array([1, 2, 3])               # array NumPy base
    y_np = np.array([4, 5])                  # otro array NumPy base
    x = da.from_array(x_np, chunks=(2,))     # Dask Array con chunks de tamaño 2
    y = da.from_array(y_np, chunks=(1,))     # Dask Array con chunks de tamaño 1
    result = kron(x, y)                      # resultado Dask
    expected = np.kron(x_np, y_np)           # resultado NumPy
    assert_eq(result, expected)              # comprueba valores y chunks

def test_kron_rectangular():
    # Test de matrices rectangulares (1×3 por 2×1):
    x_np = np.array([[1, 2, 3]])             # matriz de forma (1,3)
    y_np = np.array([[0], [1]])              # matriz de forma (2,1)
    x = da.from_array(x_np, chunks=(1, 2))   # chunks: 1 fila, 2 columnas
    y = da.from_array(y_np, chunks=(1, 1))   # chunks: 1 fila, 1 columna
    result = kron(x, y)
    expected = np.kron(x_np, y_np)
    assert_eq(result, expected)

def test_kron_empty():
    # Test de arrays con dimensión vacía:
    # Cuando uno de los arrays tiene tamaño cero, debe funcionar sin errores
    x_np = np.empty((0, 3))                  # matriz de 0 filas y 3 columnas
    y_np = np.array([[1, 2], [3, 4]])        # matriz de 2×2 normal
    x = da.from_array(x_np, chunks=(0, 3))   # chunk de 0×3
    y = da.from_array(y_np, chunks=(2, 1))   # chunk de 2 filas, 1 columna
    result = kron(x, y)
    expected = np.kron(x_np, y_np)
    assert_eq(result, expected)

@pytest.mark.parametrize("shape_x, chunks_x, shape_y, chunks_y, dtype", [
    # Parámetros de test para escenarios más “reales”:
    # 1) Matrices grandes pero un solo bloque (todo en memoria)
    ((100, 50),  (100, 50),  (20, 10),  (20, 10),  int),
    # 2) Matrices medianas con múltiples bloques
    ((30, 40),   (10, 20),   (15, 10),  (5, 5),    float),
    # 3) Vectores 1D con chunks mixtos (fuerza el fallback de NumPy)
    ((5,),       (2,),       (7,),      (3,),      int),
    # 4) Promoción de dtype: mezcla float e int
    ((10, 10),   (3, 7),     (8, 6),    (4, 2),    float),
])
def test_kron_various(shape_x, chunks_x, shape_y, chunks_y, dtype):
    """
    Parametrized test for various shapes, chunkings, and dtypes.
    Prueba parametrizada para varias formas, chunks y tipos de datos.
    """
    # Test parametrizado: recorre varias configuraciones automáticamente
    rng = np.random.default_rng(0)
    x_np = rng.integers(0, 10, size=shape_x).astype(dtype)
    y_np = rng.integers(0, 10, size=shape_y).astype(dtype)

    x = da.from_array(x_np, chunks=chunks_x)
    y = da.from_array(y_np, chunks=chunks_y)

    result = kron(x, y)
    expected = np.kron(x_np, y_np)

    # Comprueba que resultado Dask == resultado NumPy, incluyendo chunking
    assert_eq(result, expected)

def test_kron_random_property():
    """
    Smoke test with random small shapes and values.
    Prueba rápida con formas y valores aleatorios pequeños.
    """
    rng = np.random.default_rng(1)
    for _ in range(5):
        # Genera dimensiones aleatorias entre 1 y 5
        nx, ny = rng.integers(1, 6, size=2)
        mx, my = rng.integers(1, 6, size=2)
        shape_x = (nx, ny)
        shape_y = (mx, my)
        # Define chunks que dividen cada dimensión en dos (al menos 1)
        chunks_x = (max(1, nx//2), max(1, ny//2))
        chunks_y = (max(1, mx//2), max(1, my//2))

        x_np = rng.standard_normal(shape_x)   # datos float aleatorios
        y_np = rng.standard_normal(shape_y)

        x = da.from_array(x_np, chunks=chunks_x)
        y = da.from_array(y_np, chunks=chunks_y)

        # La propiedad central: kron(x,y) debe coincidir con np.kron
        assert_eq(kron(x, y), np.kron(x_np, y_np))

def test_kron_medium_reduction_compute():
    """
    Medium-weight test:
    1) Checks kron(x, y) metadata shape.
    2) Runs .mean().compute() reduction without materializing the full array.
    Prueba moderadamente pesada:
    1) Verifica la forma metadata de kron(x, y).
    2) Ejecuta una reducción .mean().compute() sin materializar todo el array.
    """
    # Creamos arrays de 1.000.000 de elementos (200×50 y 50×20)
    x_np = np.ones((200, 50), dtype=float)
    y_np = np.ones(( 50, 20), dtype=float)

    # Convertimos a Dask Array con chunks pequeños que quepan en memoria
    x = da.from_array(x_np, chunks=(50, 25))
    y = da.from_array(y_np, chunks=(25, 10))

    # Construimos el grafo de kron sin .compute() completo
    z = kron(x, y)

    # 1) Verificamos metadata: shape
    assert z.shape == (200 * 50, 50 * 20)

    # 2) Ejecutamos solo la reducción .mean()
    mean_val = z.mean().compute()
    # Como todos los valores son 1, la media debe ser 1.0
    assert mean_val == pytest.approx(1.0)