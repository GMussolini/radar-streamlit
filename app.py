from __future__ import annotations

import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

load_dotenv()
ENGINE = create_engine(
    f"mssql+pymssql://{st.secrets['DB_USER']}:{st.secrets['DB_PASS']}@"
    f"{st.secrets['DB_HOST']}/{st.secrets['DB_NAME']}",
    future=True,
    pool_pre_ping=True,
)

SQL_RESUMO = """
DECLARE @ini DATE = :d_ini, @fim DATE = :d_fim;

WITH notas AS (
    SELECT  ac.ColaboradorId,
            AVG((CAST(ac.Tecnico AS FLOAT)+CAST(ac.Comunicacao AS FLOAT)+CAST(ac.Comprometimento AS FLOAT))/3.0) AS NotaMedia
    FROM    AvaliacaoColaboradores ac
    WHERE   ac.Periodo BETWEEN @ini AND @fim
    GROUP BY ac.ColaboradorId
)
SELECT  c.Id                          AS ContratoId,
        e.Nome                        AS Cliente,
        c.Objeto                      AS Projeto,
        COUNT(DISTINCT pec.ColaboradorId)                         AS Colaboradores,
        MIN(n.NotaMedia)                                       AS PiorNota,     -- NULL se não houver nota
        AVG(n.NotaMedia)                                       AS NotaMedia,    -- NULL idem
        SUM(CASE WHEN n.NotaMedia IS NOT NULL 
                  AND n.NotaMedia < :limiar THEN 1 ELSE 0 END) AS ColabsRuins
FROM            Contratos c
JOIN            Empresas e ON e.Id = c.EmpresaId   AND c.IsAtivo = 1
JOIN            ProjetoEmpresaColaboradores pec
                ON pec.ContratoId = c.Id           AND pec.Ativo = 1
LEFT JOIN       notas n ON n.ColaboradorId = pec.ColaboradorId
GROUP BY        c.Id, e.Nome, c.Objeto;
"""

SQL_DETALHES = """
DECLARE @ini DATE = :d_ini, @fim DATE = :d_fim;

SELECT  col.NomeCompleto,
        ac.Periodo,
        (CAST(ac.Tecnico AS FLOAT)+CAST(ac.Comunicacao AS FLOAT)+CAST(ac.Comprometimento AS FLOAT))/3.0 AS Nota,
        ac.Tecnico,
        ac.Comunicacao,
        ac.Comprometimento,
        ac.Descricao
FROM            ProjetoEmpresaColaboradores pec
JOIN            AvaliacaoColaboradores ac   ON ac.ColaboradorId = pec.ColaboradorId
                                            AND ac.Periodo BETWEEN :d_ini AND :d_fim
JOIN            Colaboradores col           ON col.Id = ac.ColaboradorId
WHERE pec.ContratoId = :cid
  AND ac.ContratoId = :cid
  AND pec.Ativo = 1
ORDER BY        col.NomeCompleto, ac.Periodo DESC;
"""

def interval_month(month: pd.Timestamp) -> tuple[str, str]:
    ini = month.replace(day=1).date()
    fim = (month + relativedelta(months=1) - pd.Timedelta(days=1)).date()
    return ini, fim

def fetch_resumo(month: pd.Timestamp, limiar: float) -> pd.DataFrame:
    d_ini, d_fim = interval_month(month)
    with ENGINE.begin() as conn:
        df = pd.read_sql(
            text(SQL_RESUMO),
            conn,
            params={"d_ini": d_ini, "d_fim": d_fim, "limiar": limiar},
        )

    def classifica(row) -> tuple[int, str]:
        if pd.isna(row.PiorNota):
            return 3, "⏸️ Sem avaliação"
        if row.PiorNota < limiar - 0.5:
            return 0, "🔥 Crítico"
        if row.PiorNota < limiar:
            return 1, "⚠️ Alerta"
        return 2, "OK"

    df[["Severidade", "Status"]] = df.apply(classifica, axis=1, result_type="expand")
    df.sort_values(["Severidade", "PiorNota"], inplace=True)
    return df

def fetch_detalhes(cid: int, month: pd.Timestamp) -> pd.DataFrame:
    d_ini, d_fim = interval_month(month)
    print({"d_ini": d_ini, "d_fim": d_fim})
    with ENGINE.begin() as conn:
        return pd.read_sql(
            text(SQL_DETALHES),
            conn,
            params={"cid": cid, "d_ini": d_ini, "d_fim": d_fim},
        )

st.set_page_config("Radar de Contratos", layout="wide")
st.title("Radar de Contratos por Avaliação de Colaboradores")

meses = pd.date_range(end=pd.Timestamp.today(), periods=24, freq="MS")[::-1]
mes_str = [m.strftime("%Y-%m") for m in meses]
col_filtros = st.columns(3)
with col_filtros[0]:
    idx = st.selectbox("Mês", mes_str, index=0)
    mes = meses[mes_str.index(idx)]
with col_filtros[1]:
    LIMIAR = st.slider("Limiar de nota (ruim se <)", 0.0, 5.0, 3.0, 0.1)

df = fetch_resumo(mes, LIMIAR)

st.subheader("Contratos – ordenados pela gravidade")
st.dataframe(
    df.drop(columns="Severidade").rename(
        columns={
            "PiorNota": "Pior nota",
            "NotaMedia": "Nota média",
            "ColabsRuins": "Colabs < limiar",
        }
    ),
    hide_index=True,
    column_config={
        "Pior nota": st.column_config.NumberColumn(format="%.2f"),
        "Nota média": st.column_config.NumberColumn(format="%.2f"),
    },
    height=min(500, 35 * (len(df) + 3)),
)

cid_to_nome = {row.ContratoId: f"{row.Cliente} – {row.Projeto}" for row in df.itertuples()}
cid_default = df.iloc[0]["ContratoId"] if not df.empty else None

st.divider()
st.subheader("Detalhe do contrato")

cid = st.selectbox(
    "Selecione um contrato para ver avaliações individuais",
    options=list(cid_to_nome.keys()),
    format_func=lambda k: cid_to_nome[k],
    index=0 if cid_default else None,
)

if cid:
    detal = fetch_detalhes(cid, mes)
    st.caption(f"Avaliações de **{cid_to_nome[cid]}** em {mes:%B/%Y}")
    with st.expander("Clique para ver detalhes"):
        st.dataframe(
            detal.rename(
                columns={
                    "NomeCompleto": "Colaborador",
                    "Nota": "Nota média",
                    "Descricao": "Comentário",
                }
            ),
            hide_index=True,
            column_config={
                "Nota média": st.column_config.NumberColumn(format="%.2f"),
                "Tecnico": st.column_config.NumberColumn(format="%.0f"),
                "Comunicacao": st.column_config.NumberColumn(format="%.0f"),
                "Comprometimento": st.column_config.NumberColumn(format="%.0f"),
            },
            use_container_width=True,
        )
else:
    st.info("Nenhum contrato para exibir.")
