import cx_Oracle

if __name__ == '__main__':

    op_conexao = cx_Oracle.connect('locadora/locadora@localhost:1521/xe')
    dw_conexao = cx_Oracle.connect('dw_locadora/dw_locadora@localhost:1521/xe')

    op_cursor = op_conexao.cursor()
    dw_cursor = dw_conexao.cursor()


    def dm_socio():
        tbSocio = op_cursor.execute('select cod_soc, nom_soc, dsc_tps from socios join tipos_socios using(cod_soc)')
        tbl_socio = tbSocio.fetchall()

        for n in tbl_socio:
            dw_cursor.execute('insert into dm_socio(id_soc, nom_soc, tipo_socio) values (:1,:2,:3)', n)
            dw_conexao.commit()


    def dm_artista():
        tbArtista = op_cursor.execute('select cod_art, tpo_art, nac_bras, nom_art from artistas')
        tbl_artista = tbArtista.fetchall()

        for n in tbl_artista:
            dw_cursor.execute('insert into dm_artista(id_art, tpo_art, nas_bras, nom_art) values (:1,:2,:3,:4)', n)
            dw_conexao.commit()


    def dm_gravadora():
        tbGravadora = op_cursor.execute('select * from gravadoras')
        tbl_gravadora = tbGravadora.fetchall()

        for n in tbl_gravadora:
            dw_cursor.execute('insert into dm_gravadora(id_grav, uf_grav, nac_bras, nom_grav) values (:1,:2,:3,:4)', n)
            dw_conexao.commit()


    def dm_titulo():
        tbTitulo = op_cursor.execute('select cod_tit, tpo_tit, cla_tit, dsc_tit from titulos')
        tbl_titulo = tbTitulo.fetchall()

        for n in tbl_titulo:
            dw_cursor.execute('insert into dm_titulo(id_titulo, tpo_titulo, cla_titulo, dsc_titulo) values (:1,:2,:3,:4)', n)
            dw_conexao.commit()
    

    def dm_tempo():
        tbTempo = op_cursor.execute(
             """select distinct TO_CHAR(dat_loc, 'YY'), TO_CHAR(dat_loc, 'MM'),TO_CHAR(dat_loc, 'YY/MM'), TO_CHAR(dat_loc, 'MON'),
            TO_CHAR(dat_loc, 'MM/YY'),TO_CHAR(dat_loc, 'fmMONTH'), TO_CHAR(dat_loc, 'DD'), TO_CHAR(dat_loc) ,  
            TO_CHAR(dat_loc, 'HH24'), TO_CHAR(dat_loc, 'AM') FROM  itens_locacoes""")

        tbl_tempo = tbTempo.fetchall()

        tbl_aux = []
        id_temp = 1

        for n in tbl_tempo:
            tbl_aux.append([id_temp, n])
            id_temp += 1

        
        
        for n in tbl_aux:
            dw_cursor.execute('insert into dm_tempo(id_tempo, num_ano, nu_mes, nu_anomes, sg_mes,'
            ' nm_mesano, nm_mes, nu_dia, dt_tempo, nu_hora, turno) values (:1,:2,:3,:4,:5,:6,:7,:8,:9:10:11)', n)
            dw_conexao.commit()
    

    def ft_locacoes():
        tbItens_locacoes =  op_cursor.execute('select cod_soc, cod_tit, cod_art, cod_grav,'
        ' val_loc, DATEDIFF(day, dat_dev, dat_loc), dat_loc, from itens_locacoes')

        tbl_locacoes = tbItens_locacoes.fetchall()

        dim_tempo = dw_cursor.execute('select id_tempo, dt_tempo from dm_tempo')
        tab_tempo = dim_tempo.fetchall()

        tb_aux = []
        id_tempo = 0

        for n in tab_tempo:
            for j in tbl_locacoes:
                if n[1] == j[6]:
                    id_tempo = n[0]
                    tb_aux.append([id_tempo, j])
        
        for n in tb_aux:
            dw_cursor.execute('insert into ft_locacoes(id_tempo, id_soc, id_titulo, id_art, id_grav, valor_arrecadado, tempo_devolucao)'
            ' values (:1,:2,:3,:4,:5,:6,:7)', n)
            dw_conexao.commit()


    op_cursor.close()
    dw_cursor.close()
    op_conexao.close()
    dw_conexao.close()
