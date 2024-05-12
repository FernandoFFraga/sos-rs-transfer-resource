import streamlit as st
from pyvis.network import Network
import networkx as nx
import streamlit.components.v1 as components
import duckdb
import plotly.express as px


def get_rows(query):
    cursor = duckdb.connect("./storage/db/shelter.db")
    return cursor.sql(query).fetchall()

def get_df(query):
    cursor = duckdb.connect("./storage/db/shelter.db")
    return cursor.sql(query).df()

def side_bar():
    with st.sidebar:
        st.markdown("<h1 style='text-align: center; padding-bottom: 0px;'>SOS RS</h3>", unsafe_allow_html=True)
        st.markdown("""---""")

        st.markdown("<h3 style='text-align: center;'>Recursos</h3>", unsafe_allow_html=True)
        btn_01 = st.button('Itens Faltantes', use_container_width = True)
        btn_02 = st.button('Itens Sobressalente', use_container_width = True)
        btn_03 = st.button('Transferências', use_container_width = True)

        st.markdown("""---""")
        st.markdown("<h5 style='text-align: center;'>Ajude o Rio Grande do Sul! <a href='https://www.instagram.com/sosrs_ajuda/'>Saiba como.</a></h3>", unsafe_allow_html=True)

    if btn_01:
        page_need_donations()
    elif btn_02:
        page_remaining_supplies()
    elif btn_03:
        page_graph_resource()
    else:
        page_graph_resource()

def page_graph_resource():
    G = nx.Graph()

    for row in get_rows("SELECT name_locale, RECURSO FROM tb_cruzamento"):
        G.add_edge(row[0], row[1])

    for row in get_rows("SELECT name_locale_1, RECURSO_1 FROM tb_cruzamento"):
        G.add_edge(row[1], row[0])

    nt = Network("450px", "100%", notebook=True, directed=True)
    nt.from_nx(G)
    nt.show("./storage/graph/graph.html")

    html = open('./storage/graph/graph.html', 'r', encoding='utf-8')
    source = html.read()
    st.markdown("<h3 style='text-align: center;'>Recursos sobressalentes x faltantes</h3>", unsafe_allow_html=True)
    components.html(source, height=450)

def page_need_donations():
    df_doacao = get_df("""
        SELECT name RECURSO, count(1) QTD_ABRIGOS 
        FROM shelters 
        WHERE tags[1] = 'NeedDonations'
        GROUP BY name 
        ORDER BY QTD_ABRIGOS DESC
        LIMIT 30;
    """)

    fig_doacao = px.bar(df_doacao, x='RECURSO', y='QTD_ABRIGOS')

    st.markdown("<h3 style='text-align: center;'>Quantidade de abrigos que precisam do recurso</h3>",
                unsafe_allow_html=True)
    st.plotly_chart(fig_doacao, use_container_width=True)

def page_remaining_supplies():
    df_sobra = get_df("""
        SELECT name RECURSO, count(1) QTD_ABRIGOS 
        FROM shelters 
        WHERE tags[1] = 'RemainingSupplies'
        GROUP BY name 
        ORDER BY QTD_ABRIGOS DESC
        LIMIT 30;
    """)

    fig_sobra = px.bar(df_sobra, x='RECURSO', y='QTD_ABRIGOS')

    st.markdown("<h3 style='text-align: center;'>Quantidade de abrigos que tem o recurso sobrando</h3>", unsafe_allow_html=True)
    st.plotly_chart(fig_sobra, use_container_width = True)

def show():
    st.set_page_config(layout="wide")
    side_bar()

show()