import sys
import os
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from debentures_dot_com import EmissoesDebentures, EstoquesCorporativos, EventosFinanceiros, MercadoSecundario

def test_apis():
    print("Testing EmissoesDebentures...")
    ed = EmissoesDebentures()
    try:
        df = ed.lista_deb_publicas(timeout=30)
        print(f"lista_deb_publicas: {'OK' if not df.empty else 'EMPTY'}")
        
        if not df.empty:
            ativo = df.iloc[0]['Ativo']
            df2 = ed.lista_caracteristicas(ativo, timeout=30)
            print(f"lista_caracteristicas: {'OK' if not df2.empty else 'EMPTY'}")
            
            df3 = ed.pu_historico(ativo, timeout=30)
            print(f"pu_historico: {'OK' if not df3.empty else 'EMPTY'}")
    except Exception as e:
        print(f"Error in EmissoesDebentures: {e}")

    print("\nTesting EstoquesCorporativos...")
    ec = EstoquesCorporativos()
    try:
        df = ec.estoque_por_periodo(dt_ini='01/01/2024', dt_fim='31/01/2024', timeout=30)
        print(f"estoque_por_periodo: {'OK' if not df.empty else 'EMPTY'}")
    except Exception as e:
        print(f"Error in EstoquesCorporativos: {e}")

    print("\nTesting EventosFinanceiros...")
    ef = EventosFinanceiros()
    try:
        df = ef.agenda_eventos(dt_ini='01/01/2024', dt_fim='31/01/2024', timeout=30)
        print(f"agenda_eventos: {'OK' if not df.empty else 'EMPTY'}")
    except Exception as e:
        print(f"Error in EventosFinanceiros: {e}")

    print("\nTesting MercadoSecundario...")
    ms = MercadoSecundario()
    try:
        # Use a valid recent date for the file
        df = ms.arquivo_precos_diario('250717') # Using the date format in the dev.ipynb but adjusted for yy
        print(f"arquivo_precos_diario: {'OK' if not df.empty else 'EMPTY'}")
    except Exception as e:
        print(f"Error in MercadoSecundario: {e}")

if __name__ == '__main__':
    test_apis()
    print("\nAll tests finished.")
