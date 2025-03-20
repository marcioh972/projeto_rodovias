import streamlit as st
import pandas as pd
import sqlite3
import os
import logging
import folium
from streamlit_folium import folium_static
from coleta_dados import baixar_dados_dnit
from datetime import datetime

# ======================================
# CONFIGURAÇÃO INICIAL
# ======================================
os.makedirs("database", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename='logs/streamlit_app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# ======================================
# FUNÇÕES DO BANCO DE DADOS (COM HISTÓRICO)
# ======================================
def gerenciar_banco(df=None, ano=None, br=None, clear=False):
    """Gerencia todas as operações do banco de dados com tratamento de erros"""
    try:
        conn = sqlite3.connect("database/rodovias.db")
        cursor = conn.cursor()

        # Criar tabela principal
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dados_dnit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ano INTEGER NOT NULL,
                br INTEGER NOT NULL,
                dados TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ano, br)
            )
        ''')

        # Criar tabela de histórico
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historico (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consulta TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        if clear:
            cursor.execute("DELETE FROM dados_dnit")
            cursor.execute("DELETE FROM historico")
            conn.commit()
            return

        if df is not None:
            # Inserir dados principais
            df_json = df.to_json(orient='records')
            cursor.execute('''
                INSERT OR REPLACE INTO dados_dnit (ano, br, dados)
                VALUES (?, ?, ?)
            ''', (ano, br, df_json))

            # Registrar no histórico
            cursor.execute('''
                INSERT INTO historico (consulta)
                VALUES (?)
            ''', (f"BR-{br} ({ano})",))

            conn.commit()

        return conn

    except sqlite3.Error as e:
        logging.error(f"Erro SQLite: {str(e)}")
        raise RuntimeError("Erro no banco de dados")
    finally:
        if 'conn' in locals():
            conn.close()

# ======================================
# CACHE E CARREGAMENTO DE DADOS
# ======================================
@st.cache_data(ttl=3600, show_spinner="Carregando dados em cache...")
def carregar_dados(_df):
    """Processamento adicional de dados com cache"""
    try:
        # Converter coordenadas (exemplo hipotético)
        if 'latitude' in _df.columns and 'longitude' in _df.columns:
            _df['latitude'] = pd.to_numeric(_df['latitude'], errors='coerce')
            _df['longitude'] = pd.to_numeric(_df['longitude'], errors='coerce')
        return _df.dropna(subset=['latitude', 'longitude'], how='all')
    except Exception as e:
        logging.error(f"Erro no processamento: {str(e)}")
        return _df

# ======================================
# COMPONENTES DA INTERFACE
# ======================================
def exibir_historico():
    """Mostra o histórico de consultas na sidebar"""
    try:
        conn = sqlite3.connect("database/rodovias.db")
        historico = pd.read_sql("SELECT * FROM historico ORDER BY timestamp DESC LIMIT 10", conn)
        
        st.sidebar.subheader("📚 Histórico de Consultas")
        if not historico.empty:
            for _, row in historico.iterrows():
                st.sidebar.write(f"🗓️ {row['timestamp']} - {row['consulta']}")
        else:
            st.sidebar.write("Nenhuma consulta recente")
            
    except sqlite3.Error as e:
        st.sidebar.error("Erro ao carregar histórico")

def criar_mapa(df):
    """Gera mapa interativo com Folium"""
    try:
        if df.empty:
            raise ValueError("DataFrame vazio")
            
        m = folium.Map(location=[-15.788497, -47.879873], zoom_start=4)
        
        # Adicionar marcadores
        for idx, row in df.iterrows():
            if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=f"BR-{row['br']} | {row['uf']}",
                    icon=folium.Icon(color='blue', icon='road')
                ).add_to(m)
        
        return m
        
    except KeyError:
        logging.warning("Dados geográficos ausentes")
        return None
    except Exception as e:
        logging.error(f"Erro no mapa: {str(e)}")
        return None

# ======================================
# INTERFACE PRINCIPAL
# ======================================
st.title("🚦 Painel PNCT - DNIT")
st.markdown("### Dados Rodoviários Integrados")

# Controles principais
with st.form(key="main_form"):
    col1, col2 = st.columns(2)
    with col1:
        ano = st.number_input("Ano", min_value=2000, max_value=datetime.now().year, value=2023)
    with col2:
        br = st.number_input("Número da BR", min_value=1, max_value=999, value=101)
    
    submitted = st.form_submit_button("Buscar Dados")

# Seção de histórico
exibir_historico()

# Processamento principal
if submitted:
    try:
        with st.spinner("🚀 Buscando dados. Aguarde..."):
            # Baixar e processar dados
            df_raw = baixar_dados_dnit(ano, br)
            df_processed = carregar_dados(df_raw)
            
            # Salvar no banco
            gerenciar_banco(df=df_processed, ano=ano, br=br)
            
        st.success(f"✅ {len(df_processed)} registros carregados com sucesso!")
        
        # Visualização de dados
        st.header("📊 Visualização dos Dados")
        st.dataframe(df_processed.head(10), use_container_width=True)
        
        # Filtros dinâmicos
        st.sidebar.header("⚙️ Filtros Avançados")
        ufs_disponiveis = df_processed['uf'].unique()
        ufs_selecionadas = st.sidebar.multiselect(
            "Estados (UF)",
            options=ufs_disponiveis,
            default=ufs_disponiveis[:2]
        )
        
        df_filtrado = df_processed[df_processed['uf'].isin(ufs_selecionadas)]
        
        # Mapa interativo
        st.header("🗺️ Mapa Georreferenciado")
        mapa = criar_mapa(df_filtrado)
        if mapa:
            folium_static(mapa, width=1200)
        else:
            st.warning("Dados geográficos não disponíveis para visualização")
            
        # Gráficos
        st.header("📈 Análise Estatística")
        fig = px.histogram(df_filtrado, x='uf', title="Distribuição por UF")
        st.plotly_chart(fig, use_container_width=True)
        
        # Download
        st.header("📥 Exportação de Dados")
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Baixar CSV Filtrado",
            data=csv,
            file_name=f"dados_br{br}_{ano}.csv",
            mime="text/csv"
        )
        
    except FileNotFoundError as e:
        st.error(f"🚫 Dados não encontrados: {str(e)}")
        st.markdown("""
            Verifique:
            1. A BR selecionada existe
            2. O ano possui dados disponíveis
            3. Conexão com internet ativa
        """)
    except RuntimeError as e:
        st.error(f"⚠️ Erro crítico: {str(e)}")
    except Exception as e:
        st.error("❌ Erro inesperado. Consulte os logs técnicos.")
        logging.exception("Erro não tratado:")

# Controles administrativos
st.sidebar.header("⚙️ Administração")
if st.sidebar.button("🔄 Limpar Cache e Histórico"):
    try:
        gerenciar_banco(clear=True)
        st.cache_data.clear()
        st.sidebar.success("Cache e histórico resetados")
    except Exception as e:
        st.sidebar.error("Erro na limpeza: " + str(e))