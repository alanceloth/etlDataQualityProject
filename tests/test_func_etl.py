import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
from app.etl import transform

def test_calculo_valor_total_estoque():
    """
    Test for the calculation of the total stock value, including preparation, action, and verification steps.
    """
    # Preparação
    df = pd.DataFrame({
        'id_produto': [1, 2],
        'nome': ['blusa', 'camisa'],
        'quantidade': [10, 5],
        'preco': [20.0, 100.0],
        'categoria': ['brinquedos', 'eletrônicos'],
        'email': ['produtoA@example.com', 'produtoB@example.com']
    })
    expected = pd.Series([200.0, 500.0], name='valor_total_estoque')

    # Ação
    result = transform(df)

    # Verificação
    pd.testing.assert_series_equal(result['valor_total_estoque'], expected)

def test_normalizacao_categoria():
    """
    Test function for normalizing category data.
    """
    # Preparação
    df = pd.DataFrame({
        'id_produto': [1, 2],
        'nome': ['blusa', 'camisa'],
        'quantidade': [1, 2],
        'preco': [10.0, 20.0],
        'categoria': ['brinquedos', 'eletrônicos'],
        'email': ['produtoA@example.com', 'produtoB@example.com']
    })
    expected = pd.Series(['BRINQUEDOS', 'ELETRÔNICOS'], name='categoria_normalizada')

    # Ação
    result = transform(df)

    # Verificação
    pd.testing.assert_series_equal(result['categoria_normalizada'], expected)

def test_determinacao_disponibilidade():
    """
    Test function for determining availability. Sets up a test DataFrame and an expected result, then calls the transform function and verifies the result using pd.testing.assert_series_equal.
    """
    # Preparação
    df = pd.DataFrame({
        'id_produto': [1, 2],
        'nome': ['blusa', 'camisa'],
        'quantidade': [0, 2],
        'preco': [10.0, 20.0],
        'categoria': ['brinquedos', 'eletrônicos'],
        'email': ['produtoA@example.com', 'produtoB@example.com']
    })
    expected = pd.Series([False, True], name='disponibilidade')

    # Ação
    result = transform(df)

    # Verificação
    pd.testing.assert_series_equal(result['disponibilidade'], expected)

# Para rodar os testes, execute `pytest nome_do_arquivo.py` no terminal.