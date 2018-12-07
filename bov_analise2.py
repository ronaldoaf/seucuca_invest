# -*- coding: cp1252 -*-

import sqlite3
import numpy as np



#CONFIG
INTERVALO_VARIANCIA=245
PREGAO_INICIAL=INTERVALO_VARIANCIA+1
QTD_DE_ATIVOS=40

banca=45382.61
conn = sqlite3.connect('bovespa_acoes.db')
c = conn.cursor()





for ii in range(1000):
    acoes=[]
    for e in c.execute("SELECT a.CODNEG, 1.0*SUM(a.TOTNEG)/SUM(t.TOTNEG) AS TOTNEG, SUM(a.VOLTOT)/SUM(t.VOLTOT) AS VOLTOT  FROM ativos a INNER JOIN totais_por_data t ON a.DATA=t.DATA   AND  a.pregao_id>=? AND a.pregao_id<? GROUP BY a.CODNEG",[PREGAO_INICIAL-INTERVALO_VARIANCIA, PREGAO_INICIAL ]).fetchall():
        acoes+=[{ 'CODNEG': e[0],'IN': (e[1]*e[2]**1)**(1.0/2) } ]
        
    precos_no_dia0={}
    for e in c.execute("SELECT CODNEG, PREULT  FROM ativos WHERE pregao_id=?",[PREGAO_INICIAL]).fetchall():
        precos_no_dia0[e[0]]=e[1]

    contagem_de_pregoes={}
    for e in c.execute("SELECT CODNEG, COUNT(TOTNEG) FROM ativos WHERE  pregao_id>=? AND pregao_id<? GROUP BY CODNEG",[PREGAO_INICIAL-INTERVALO_VARIANCIA, PREGAO_INICIAL ]).fetchall():
        if e[1]>=0.8*INTERVALO_VARIANCIA:
            contagem_de_pregoes[e[0]]=e[1]

    acoes=[acao for acao in acoes if acao['CODNEG'] in precos_no_dia0]
    acoes=[acao for acao in acoes if acao['CODNEG'] in contagem_de_pregoes]

    acoes=sorted(acoes,key=lambda x: -x['IN'])[:QTD_DE_ATIVOS]






    selecao=[acao['CODNEG'] for acao in acoes]

    medias=[]
    tabela_medias=[]
    for acao in selecao:

        precos=[e[0] for e in c.execute("SELECT PREULT  FROM ativos WHERE  pregao_id>=? AND pregao_id<? AND CODNEG=? ORDER BY DATA",[PREGAO_INICIAL-INTERVALO_VARIANCIA,PREGAO_INICIAL,acao]).fetchall()]

        diffs=[ precos[i+1]/precos[i]-1 for i in range(len(precos)-1)  ]

        media=np.mean(diffs)
        media=1
        variancia=np.var(diffs)
        #variancia=1
        medias+=[media]


        tabela_medias+=[[acao,media,variancia]]


    
    if min(medias)<0:
        for i in range(len(tabela_medias)):
            tabela_medias[i][1]+=-min(medias)

    
    
    soma_kelly=sum(e[1]/e[2] for e in tabela_medias)
    
    proporcoes=[ (e[0], e[1]/e[2]/soma_kelly) for e in tabela_medias  ]

    
    
    carteira=[]
    for proporcao in proporcoes:
        if proporcao[1]>0:
            ultimo_preco=c.execute("SELECT CODNEG,PREULT  FROM ativos WHERE pregao_id=? AND CODNEG=?",[PREGAO_INICIAL,proporcao[0]]).fetchall()[0][1]
            carteira+=[{'acao':proporcao[0], 'qtd':banca*proporcao[1]/ultimo_preco }]

    #print(carteira)


    precos_no_dia1={}
    for e in c.execute("SELECT CODNEG, PREULT  FROM ativos WHERE pregao_id=?",[PREGAO_INICIAL+1]).fetchall():
        precos_no_dia1[e[0]]=e[1]

    try:
        banca=sum(acao['qtd']*precos_no_dia1[acao['acao']] for acao in carteira  )  
    except:
        print('quebrou')
    DATA=c.execute("SELECT DATA  FROM ativos WHERE pregao_id=? LIMIT 1",[PREGAO_INICIAL+1]).fetchall()[0][0]

    print(DATA,banca)


    PREGAO_INICIAL+=1



