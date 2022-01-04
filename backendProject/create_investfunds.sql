SET FOREIGN_KEY_CHECKS=0;

CREATE DATABASE IF NOT EXISTS `investfunds` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE investfunds;

CREATE TABLE IF NOT EXISTS `administrador_fundos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cnpj` varchar(20) NOT NULL,
  `nome` varchar(100) NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_CNPJ` (`cnpj`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `auditor_cias_abertas` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cnpj` varchar(20) NOT NULL,
  `nome` varchar(100) NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_CNPJ` (`cnpj`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cadastro_fundos` (
  `cnpj_fundo_id` int(11) NOT NULL,
  `DT_CONST` date NOT NULL,
  `tipo_classe_fundo_id` int(11) NOT NULL,
  `DT_INI_CLASSE` date NOT NULL,
  `tipo_rentabilidade_fundo_id` int(11) NOT NULL,
  `CONDOM_ABERTO` tinyint(1) NOT NULL,
  `FUNDO_COTAS` tinyint(1) NOT NULL,
  `FUNDO_EXCLUSIVO` tinyint(1) NOT NULL,
  `INVEST_QUALIF` tinyint(1) NOT NULL,
  `gestor_fundo_id` int(11) NOT NULL,
  `administrador_fundo_id` int(11) NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`cnpj_fundo_id`,`tipo_classe_fundo_id`,`DT_INI_CLASSE`,`tipo_rentabilidade_fundo_id`,`CONDOM_ABERTO`,`FUNDO_COTAS`,`FUNDO_EXCLUSIVO`,`INVEST_QUALIF`,`gestor_fundo_id`,`administrador_fundo_id`) USING BTREE,
  KEY `IDX_ID_FUNDO` (`cnpj_fundo_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cancelamento_cias_abertas` (
  `cnpj_fundo_id` int(11) NOT NULL,
  `DT_CANCEL` date NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`cnpj_fundo_id`,`DT_CANCEL`) USING BTREE,
  KEY `IDX_ID_FUNDO` (`cnpj_fundo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cancelamento_fundos` (
  `cnpj_fundo_id` int(11) NOT NULL,
  `DT_CANCEL` date NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`cnpj_fundo_id`,`DT_CANCEL`) USING BTREE,
  KEY `IDX_ID_FUNDO` (`cnpj_fundo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `carteiras_investimentos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `usuario_id` int(11) NOT NULL,
  `nome` varchar(100) NOT NULL,
  `descricao` varchar(200) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `IDX_USUARIO_ID` (`usuario_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cias_bpas` (
  `CNPJ_CIA` varchar(20) NOT NULL,
  `DT_REFER` date NOT NULL,
  `VERSAO` int(5) DEFAULT NULL,
  `CD_CVM` int(7) DEFAULT NULL,
  `MOEDA` varchar(4) DEFAULT NULL,
  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,
  `ORDEM_EXERC` varchar(9) NOT NULL,
  `DT_FIM_EXERC` date DEFAULT NULL,
  `CD_CONTA` varchar(18) NOT NULL,
  `DS_CONTA` varchar(100) DEFAULT NULL,
  `VL_CONTA` decimal(29,10) DEFAULT NULL,
  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,
  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`CD_CONTA`,`ORDEM_EXERC`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cias_bpps` (
  `CNPJ_CIA` varchar(20) NOT NULL,
  `DT_REFER` date NOT NULL,
  `VERSAO` int(5) DEFAULT NULL,
  `CD_CVM` int(7) DEFAULT NULL,
  `MOEDA` varchar(4) DEFAULT NULL,
  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,
  `ORDEM_EXERC` varchar(9) NOT NULL,
  `DT_FIM_EXERC` date DEFAULT NULL,
  `CD_CONTA` varchar(18) NOT NULL,
  `DS_CONTA` varchar(100) DEFAULT NULL,
  `VL_CONTA` decimal(29,10) DEFAULT NULL,
  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,
  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`CD_CONTA`,`ORDEM_EXERC`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cias_dfcs_mds` (
  `CNPJ_CIA` varchar(20) NOT NULL,
  `DT_REFER` date NOT NULL,
  `VERSAO` int(5) DEFAULT NULL,
  `CD_CVM` decimal(7,0) DEFAULT NULL,
  `MOEDA` varchar(4) DEFAULT NULL,
  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,
  `ORDEM_EXERC` varchar(9) DEFAULT NULL,
  `DT_INI_EXERC` date NOT NULL,
  `DT_FIM_EXERC` date NOT NULL,
  `CD_CONTA` varchar(18) NOT NULL,
  `DS_CONTA` varchar(100) DEFAULT NULL,
  `VL_CONTA` decimal(29,10) DEFAULT NULL,
  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,
  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`DT_INI_EXERC`,`DT_FIM_EXERC`,`CD_CONTA`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cias_dmpls` (
  `CNPJ_CIA` varchar(20) NOT NULL,
  `DT_REFER` date NOT NULL,
  `VERSAO` int(5) DEFAULT NULL,
  `CD_CVM` decimal(7,0) DEFAULT NULL,
  `MOEDA` varchar(4) DEFAULT NULL,
  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,
  `ORDEM_EXERC` varchar(9) DEFAULT NULL,
  `DT_INI_EXERC` date NOT NULL,
  `DT_FIM_EXERC` date NOT NULL,
  `COLUNA_DF` varchar(60) DEFAULT NULL,
  `CD_CONTA` varchar(18) NOT NULL,
  `DS_CONTA` varchar(100) DEFAULT NULL,
  `VL_CONTA` decimal(29,10) DEFAULT NULL,
  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,
  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`DT_INI_EXERC`,`DT_FIM_EXERC`,`CD_CONTA`),
  KEY `dmpl_cia_aberta_con_CNPJ_CIA_IDX` (`CNPJ_CIA`,`DT_INI_EXERC`,`DT_FIM_EXERC`,`DS_CONTA`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cias_dres` (
  `CNPJ_CIA` varchar(20) NOT NULL,
  `DT_REFER` date NOT NULL,
  `VERSAO` int(5) DEFAULT NULL,
  `CD_CVM` decimal(7,0) DEFAULT NULL,
  `MOEDA` varchar(4) DEFAULT NULL,
  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,
  `ORDEM_EXERC` varchar(9) DEFAULT NULL,
  `DT_INI_EXERC` date NOT NULL,
  `DT_FIM_EXERC` date NOT NULL,
  `CD_CONTA` varchar(18) NOT NULL,
  `DS_CONTA` varchar(100) DEFAULT NULL,
  `VL_CONTA` decimal(29,10) DEFAULT NULL,
  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,
  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`CD_CONTA`,`DT_INI_EXERC`,`DT_FIM_EXERC`),
  KEY `dre_cia_aberta_con_DT_FIM_EXERC_IDX` (`DT_FIM_EXERC`) USING BTREE,
  KEY `dre_cia_aberta_con_DT_INI_EXERC_IDX` (`DT_INI_EXERC`) USING BTREE,
  KEY `dre_cia_aberta_con_DS_CONTA_IDX` (`DS_CONTA`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cias_indicadores` (
  `CNPJ_CIA` varchar(20) NOT NULL,
  `DT_REFER` date NOT NULL,
  `CD_B3` varchar(20) NOT NULL,
  `CLOSE_PRICE` decimal(29,10) DEFAULT NULL,
  `EBIT` decimal(29,10) DEFAULT NULL,
  `MARGEM_EBIT%` decimal(29,10) DEFAULT NULL,
  `EBITDA` decimal(29,10) DEFAULT NULL,
  `MARGEM_EBITDA%` decimal(29,10) DEFAULT NULL,
  `P/L` decimal(29,10) DEFAULT NULL,
  `P/VP` decimal(29,10) DEFAULT NULL,
  `P/EBIT` decimal(29,10) DEFAULT NULL,
  `P/EBITDA` decimal(29,10) DEFAULT NULL,
  `EV/EBIT` decimal(29,10) DEFAULT NULL,
  `EV/EBITDA` decimal(29,10) DEFAULT NULL,
  `EV` decimal(29,10) DEFAULT NULL,
  `VPA` decimal(29,10) DEFAULT NULL,
  `ROE%` decimal(29,10) DEFAULT NULL,
  `ROIC%` decimal(29,10) DEFAULT NULL,
  `LPA` decimal(29,10) DEFAULT NULL,
  `EBIT/ATIVO%` decimal(29,10) DEFAULT NULL,
  `GIRO_ATIVO` decimal(29,10) DEFAULT NULL,
  `MARGEM_LIQUIDA%` decimal(29,10) DEFAULT NULL,
  `LIQUIDEZ_CORRENTE` decimal(29,10) DEFAULT NULL,
  `LUCRO_LIQUIDO` decimal(29,10) DEFAULT NULL,
  `RECEITA_LIQUIDA` decimal(29,10) DEFAULT NULL,
  `PATRIMONIO_LIQUIDO` decimal(29,10) DEFAULT NULL,
  `PASSIVO` decimal(29,10) DEFAULT NULL,
  `ATIVO` decimal(29,10) DEFAULT NULL,
  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`CD_B3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cias_precos_diarios` (
  `CD_B3` varchar(20) NOT NULL,
  `DT_REFER` date NOT NULL,
  `COD_BDI` varchar(20) DEFAULT NULL,
  `DENOM_CIA` varchar(100) DEFAULT NULL,
  `SPEC_CODE` varchar(11) DEFAULT NULL,
  `CLOSE_PRICE` decimal(29,10) DEFAULT NULL,
  `OPEN_PRICE` decimal(29,10) DEFAULT NULL,
  `MINIMUM_PRICE` decimal(29,10) NOT NULL,
  `MAXIMUM_PRICE` decimal(29,10) NOT NULL,
  PRIMARY KEY (`CD_B3`,`DT_REFER`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cnpj_cias_abertas` (
  `CNPJ_CIA` varchar(20) NOT NULL,
  `DENOM_SOCIAL` varchar(100) NOT NULL,
  `DENOM_COMERC` varchar(100) NOT NULL,
  `DT_REG` date NOT NULL,
  `DT_CONST` date NOT NULL,
  `DT_CANCEL` date NOT NULL,
  `DT_INI_SIT` date NOT NULL,
  `CD_CVM` int(11) NOT NULL,
  `SIT` varchar(40) NOT NULL,
  `SETOR_ATIV` varchar(100) NOT NULL,
  `tipo_situacao_fundos_id` int(11) NOT NULL,
  `tipo_setor_ativ_cias_abertas_id` int(11) NOT NULL,
  PRIMARY KEY (`CNPJ_CIA`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cnpj_cias_abertas_cod_b3` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `CD_B3` varchar(20) NOT NULL,
  `CNPJ_CIA` varchar(20) DEFAULT NULL,
  `DENOM_CIA` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `cnpj_cias_abertas_cod_b3_FK` (`CNPJ_CIA`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cnpj_fundos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `CNPJ` varchar(20) NOT NULL,
  `DENOM_SOCIAL` varchar(100) NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_CNPJ` (`CNPJ`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cnpj_fundos_nao_cadastrados` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `CNPJ` varchar(20) NOT NULL,
  `DENOM_SOCIAL` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_CNPJ` (`CNPJ`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `cnpj_intermediarios` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `CNPJ` varchar(20) NOT NULL,
  `DENOM_SOCIAL` varchar(100) NOT NULL,
  `DENOM_COMERCIAL` varchar(100) NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_CNPJ` (`CNPJ`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `distribuidor_fundos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cnpj` varchar(20) NOT NULL,
  `nome` varchar(100) NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_CNPJ` (`cnpj`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `doc_extratos_fundos` (
  `cnpj_fundo_id` int(11) NOT NULL,
  `DT_COMPTC` date NOT NULL,
  `tipo_anbima_classe_id` int(11) NOT NULL,
  `APLIC_MIN` decimal(15,2) NOT NULL,
  `QT_DIA_PAGTO_COTA` int(10) NOT NULL,
  `QT_DIA_PAGTO_RESGATE` int(10) NOT NULL,
  `PR_COTA_ETF_MAX` int(10) NOT NULL,
  `tipo_benchmark_id` int(11) NOT NULL,
  PRIMARY KEY (`cnpj_fundo_id`,`DT_COMPTC`),
  KEY `IDX_ID_FUNDO` (`cnpj_fundo_id`),
  KEY `IDX_APLIC_MIN` (`APLIC_MIN`),
  KEY `IDX_ID_CLASSE_ANBIMA` (`tipo_anbima_classe_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `doc_inf_diario_fundos` (
  `cnpj_fundo_id` int(11) NOT NULL,
  `DT_COMPTC` date NOT NULL,
  `VL_TOTAL` decimal(17,2) NOT NULL,
  `VL_QUOTA` decimal(27,12) NOT NULL,
  `VL_PATRIM_LIQ` decimal(17,2) NOT NULL,
  `CAPTC_DIA` decimal(17,2) NOT NULL,
  `RESG_DIA` decimal(17,2) NOT NULL,
  `NR_COTST` int(10) NOT NULL,
  `rentab_diaria` decimal(27,12) DEFAULT NULL,
  `volat_diaria` decimal(27,12) DEFAULT NULL,
  `rentab_acumulada` decimal(27,12) DEFAULT NULL,
  `drawdown` decimal(27,12) DEFAULT NULL,
  PRIMARY KEY (`cnpj_fundo_id`,`DT_COMPTC`) USING BTREE,
  UNIQUE KEY `IDX_ID_DT_COMPTC` (`cnpj_fundo_id`,`DT_COMPTC`) USING BTREE,
  KEY `IDX_CNPJ` (`cnpj_fundo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY HASH (`cnpj_fundo_id`)
(
PARTITION p0 ENGINE=InnoDB,
PARTITION p1 ENGINE=InnoDB,
PARTITION p2 ENGINE=InnoDB,
PARTITION p3 ENGINE=InnoDB,
PARTITION p4 ENGINE=InnoDB,
PARTITION p5 ENGINE=InnoDB,
PARTITION p6 ENGINE=InnoDB,
PARTITION p7 ENGINE=InnoDB,
PARTITION p8 ENGINE=InnoDB,
PARTITION p9 ENGINE=InnoDB,
PARTITION p10 ENGINE=InnoDB,
PARTITION p11 ENGINE=InnoDB,
PARTITION p12 ENGINE=InnoDB,
PARTITION p13 ENGINE=InnoDB,
PARTITION p14 ENGINE=InnoDB,
PARTITION p15 ENGINE=InnoDB,
PARTITION p16 ENGINE=InnoDB,
PARTITION p17 ENGINE=InnoDB,
PARTITION p18 ENGINE=InnoDB,
PARTITION p19 ENGINE=InnoDB,
PARTITION p20 ENGINE=InnoDB,
PARTITION p21 ENGINE=InnoDB,
PARTITION p22 ENGINE=InnoDB,
PARTITION p23 ENGINE=InnoDB,
PARTITION p24 ENGINE=InnoDB,
PARTITION p25 ENGINE=InnoDB,
PARTITION p26 ENGINE=InnoDB,
PARTITION p27 ENGINE=InnoDB,
PARTITION p28 ENGINE=InnoDB,
PARTITION p29 ENGINE=InnoDB,
PARTITION p30 ENGINE=InnoDB,
PARTITION p31 ENGINE=InnoDB,
PARTITION p32 ENGINE=InnoDB,
PARTITION p33 ENGINE=InnoDB,
PARTITION p34 ENGINE=InnoDB,
PARTITION p35 ENGINE=InnoDB,
PARTITION p36 ENGINE=InnoDB,
PARTITION p37 ENGINE=InnoDB,
PARTITION p38 ENGINE=InnoDB,
PARTITION p39 ENGINE=InnoDB,
PARTITION p40 ENGINE=InnoDB,
PARTITION p41 ENGINE=InnoDB,
PARTITION p42 ENGINE=InnoDB,
PARTITION p43 ENGINE=InnoDB,
PARTITION p44 ENGINE=InnoDB,
PARTITION p45 ENGINE=InnoDB,
PARTITION p46 ENGINE=InnoDB,
PARTITION p47 ENGINE=InnoDB,
PARTITION p48 ENGINE=InnoDB,
PARTITION p49 ENGINE=InnoDB,
PARTITION p50 ENGINE=InnoDB,
PARTITION p51 ENGINE=InnoDB,
PARTITION p52 ENGINE=InnoDB,
PARTITION p53 ENGINE=InnoDB,
PARTITION p54 ENGINE=InnoDB,
PARTITION p55 ENGINE=InnoDB,
PARTITION p56 ENGINE=InnoDB,
PARTITION p57 ENGINE=InnoDB,
PARTITION p58 ENGINE=InnoDB,
PARTITION p59 ENGINE=InnoDB,
PARTITION p60 ENGINE=InnoDB,
PARTITION p61 ENGINE=InnoDB,
PARTITION p62 ENGINE=InnoDB,
PARTITION p63 ENGINE=InnoDB,
PARTITION p64 ENGINE=InnoDB,
PARTITION p65 ENGINE=InnoDB,
PARTITION p66 ENGINE=InnoDB,
PARTITION p67 ENGINE=InnoDB,
PARTITION p68 ENGINE=InnoDB,
PARTITION p69 ENGINE=InnoDB,
PARTITION p70 ENGINE=InnoDB,
PARTITION p71 ENGINE=InnoDB,
PARTITION p72 ENGINE=InnoDB,
PARTITION p73 ENGINE=InnoDB,
PARTITION p74 ENGINE=InnoDB,
PARTITION p75 ENGINE=InnoDB,
PARTITION p76 ENGINE=InnoDB,
PARTITION p77 ENGINE=InnoDB,
PARTITION p78 ENGINE=InnoDB,
PARTITION p79 ENGINE=InnoDB,
PARTITION p80 ENGINE=InnoDB,
PARTITION p81 ENGINE=InnoDB,
PARTITION p82 ENGINE=InnoDB,
PARTITION p83 ENGINE=InnoDB,
PARTITION p84 ENGINE=InnoDB,
PARTITION p85 ENGINE=InnoDB,
PARTITION p86 ENGINE=InnoDB,
PARTITION p87 ENGINE=InnoDB,
PARTITION p88 ENGINE=InnoDB,
PARTITION p89 ENGINE=InnoDB,
PARTITION p90 ENGINE=InnoDB,
PARTITION p91 ENGINE=InnoDB,
PARTITION p92 ENGINE=InnoDB,
PARTITION p93 ENGINE=InnoDB,
PARTITION p94 ENGINE=InnoDB,
PARTITION p95 ENGINE=InnoDB,
PARTITION p96 ENGINE=InnoDB,
PARTITION p97 ENGINE=InnoDB,
PARTITION p98 ENGINE=InnoDB,
PARTITION p99 ENGINE=InnoDB
);

CREATE TABLE IF NOT EXISTS `FUNDO_DOC_INF_DIARIO_BASE` (
  `FK_ID_FUNDO` int(11) NOT NULL,
  `DT_COMPTC` date NOT NULL,
  `VL_TOTAL` decimal(17,2) NOT NULL,
  `VL_QUOTA` decimal(27,12) NOT NULL,
  `VL_PATRIM_LIQ` decimal(17,2) NOT NULL,
  `CAPTC_DIA` decimal(17,2) NOT NULL,
  `RESG_DIA` decimal(17,2) NOT NULL,
  `NR_COTST` int(10) NOT NULL,
  `rentab_diaria` decimal(27,12) DEFAULT NULL,
  `volat_diaria` decimal(27,12) DEFAULT NULL,
  UNIQUE KEY `UNIQUE_VALS` (`FK_ID_FUNDO`,`DT_COMPTC`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY HASH (`FK_ID_FUNDO`)
(
PARTITION p0 ENGINE=InnoDB,
PARTITION p1 ENGINE=InnoDB,
PARTITION p2 ENGINE=InnoDB,
PARTITION p3 ENGINE=InnoDB,
PARTITION p4 ENGINE=InnoDB,
PARTITION p5 ENGINE=InnoDB,
PARTITION p6 ENGINE=InnoDB,
PARTITION p7 ENGINE=InnoDB,
PARTITION p8 ENGINE=InnoDB,
PARTITION p9 ENGINE=InnoDB,
PARTITION p10 ENGINE=InnoDB,
PARTITION p11 ENGINE=InnoDB,
PARTITION p12 ENGINE=InnoDB,
PARTITION p13 ENGINE=InnoDB,
PARTITION p14 ENGINE=InnoDB,
PARTITION p15 ENGINE=InnoDB,
PARTITION p16 ENGINE=InnoDB,
PARTITION p17 ENGINE=InnoDB,
PARTITION p18 ENGINE=InnoDB,
PARTITION p19 ENGINE=InnoDB,
PARTITION p20 ENGINE=InnoDB,
PARTITION p21 ENGINE=InnoDB,
PARTITION p22 ENGINE=InnoDB,
PARTITION p23 ENGINE=InnoDB,
PARTITION p24 ENGINE=InnoDB,
PARTITION p25 ENGINE=InnoDB,
PARTITION p26 ENGINE=InnoDB,
PARTITION p27 ENGINE=InnoDB,
PARTITION p28 ENGINE=InnoDB,
PARTITION p29 ENGINE=InnoDB,
PARTITION p30 ENGINE=InnoDB,
PARTITION p31 ENGINE=InnoDB,
PARTITION p32 ENGINE=InnoDB,
PARTITION p33 ENGINE=InnoDB,
PARTITION p34 ENGINE=InnoDB,
PARTITION p35 ENGINE=InnoDB,
PARTITION p36 ENGINE=InnoDB,
PARTITION p37 ENGINE=InnoDB,
PARTITION p38 ENGINE=InnoDB,
PARTITION p39 ENGINE=InnoDB,
PARTITION p40 ENGINE=InnoDB,
PARTITION p41 ENGINE=InnoDB,
PARTITION p42 ENGINE=InnoDB,
PARTITION p43 ENGINE=InnoDB,
PARTITION p44 ENGINE=InnoDB,
PARTITION p45 ENGINE=InnoDB,
PARTITION p46 ENGINE=InnoDB,
PARTITION p47 ENGINE=InnoDB,
PARTITION p48 ENGINE=InnoDB,
PARTITION p49 ENGINE=InnoDB,
PARTITION p50 ENGINE=InnoDB,
PARTITION p51 ENGINE=InnoDB,
PARTITION p52 ENGINE=InnoDB,
PARTITION p53 ENGINE=InnoDB,
PARTITION p54 ENGINE=InnoDB,
PARTITION p55 ENGINE=InnoDB,
PARTITION p56 ENGINE=InnoDB,
PARTITION p57 ENGINE=InnoDB,
PARTITION p58 ENGINE=InnoDB,
PARTITION p59 ENGINE=InnoDB,
PARTITION p60 ENGINE=InnoDB,
PARTITION p61 ENGINE=InnoDB,
PARTITION p62 ENGINE=InnoDB,
PARTITION p63 ENGINE=InnoDB,
PARTITION p64 ENGINE=InnoDB,
PARTITION p65 ENGINE=InnoDB,
PARTITION p66 ENGINE=InnoDB,
PARTITION p67 ENGINE=InnoDB,
PARTITION p68 ENGINE=InnoDB,
PARTITION p69 ENGINE=InnoDB,
PARTITION p70 ENGINE=InnoDB,
PARTITION p71 ENGINE=InnoDB,
PARTITION p72 ENGINE=InnoDB,
PARTITION p73 ENGINE=InnoDB,
PARTITION p74 ENGINE=InnoDB,
PARTITION p75 ENGINE=InnoDB,
PARTITION p76 ENGINE=InnoDB,
PARTITION p77 ENGINE=InnoDB,
PARTITION p78 ENGINE=InnoDB,
PARTITION p79 ENGINE=InnoDB,
PARTITION p80 ENGINE=InnoDB,
PARTITION p81 ENGINE=InnoDB,
PARTITION p82 ENGINE=InnoDB,
PARTITION p83 ENGINE=InnoDB,
PARTITION p84 ENGINE=InnoDB,
PARTITION p85 ENGINE=InnoDB,
PARTITION p86 ENGINE=InnoDB,
PARTITION p87 ENGINE=InnoDB,
PARTITION p88 ENGINE=InnoDB,
PARTITION p89 ENGINE=InnoDB,
PARTITION p90 ENGINE=InnoDB,
PARTITION p91 ENGINE=InnoDB,
PARTITION p92 ENGINE=InnoDB,
PARTITION p93 ENGINE=InnoDB,
PARTITION p94 ENGINE=InnoDB,
PARTITION p95 ENGINE=InnoDB,
PARTITION p96 ENGINE=InnoDB,
PARTITION p97 ENGINE=InnoDB,
PARTITION p98 ENGINE=InnoDB,
PARTITION p99 ENGINE=InnoDB
);

CREATE TABLE IF NOT EXISTS `gestor_fundos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cnpj` varchar(20) NOT NULL,
  `nome` varchar(100) NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_CNPJ` (`cnpj`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `indicadores_carteiras` (
  `carteiras_investimento_id` int(11) NOT NULL,
  `periodo_meses` int(11) NOT NULL,
  `data_final` date NOT NULL DEFAULT current_timestamp(),
  `rentabilidade` decimal(27,12) NOT NULL,
  `desvio_padrao` decimal(27,12) NOT NULL,
  `num_valores` int(11) DEFAULT NULL,
  `rentab_min` decimal(27,12) DEFAULT NULL,
  `rentab_max` decimal(27,12) DEFAULT NULL,
  `max_drawdown` decimal(27,12) NOT NULL,
  `tipo_benchmark_id` int(11) DEFAULT NULL,
  `meses_acima_bench` int(11) DEFAULT NULL,
  `sharpe` decimal(27,12) DEFAULT NULL,
  `beta` decimal(27,12) DEFAULT NULL,
  PRIMARY KEY (`carteiras_investimento_id`,`periodo_meses`,`data_final`),
  UNIQUE KEY `UNIQUE_VALS` (`carteiras_investimento_id`,`periodo_meses`,`data_final`) USING BTREE,
  KEY `IDX_DATA_FINAL` (`data_final`) USING BTREE,
  KEY `IDX_CNPJ` (`carteiras_investimento_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `indicadores_fundos` (
  `cnpj_fundo_id` int(11) NOT NULL,
  `periodo_meses` int(11) NOT NULL,
  `data_final` date NOT NULL DEFAULT current_timestamp(),
  `rentabilidade` decimal(27,12) NOT NULL,
  `desvio_padrao` decimal(27,12) NOT NULL,
  `num_valores` int(11) DEFAULT NULL,
  `rentab_min` decimal(27,12) DEFAULT NULL,
  `rentab_max` decimal(27,12) DEFAULT NULL,
  `max_drawdown` decimal(27,12) NOT NULL,
  `meses_acima_bench` int(11) DEFAULT NULL,
  `sharpe` decimal(27,12) DEFAULT NULL,
  `sharpe_geral_bench` decimal(27,12) DEFAULT NULL,
  `sharpe_geral_classe` decimal(27,12) DEFAULT NULL,
  `beta` decimal(27,12) DEFAULT NULL,
  PRIMARY KEY (`cnpj_fundo_id`,`periodo_meses`,`data_final`),
  UNIQUE KEY `UNIQUE_VALS` (`cnpj_fundo_id`,`periodo_meses`,`data_final`) USING BTREE,
  KEY `IDX_DATA_FINAL` (`data_final`) USING BTREE,
  KEY `IDX_CNPJ` (`cnpj_fundo_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `intermediarios` (
  `cnpj_intermediario_id` int(11) NOT NULL,
  `tipo_participante_id` int(11) NOT NULL,
  `dt_cancel` date DEFAULT NULL,
  `tipo_situacao_fundo_id` int(11) NOT NULL,
  `dt_ini_sit` date NOT NULL,
  `cd_cvm` int(11) NOT NULL,
  PRIMARY KEY (`cnpj_intermediario_id`,`tipo_participante_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `logs_atividades` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `action` varchar(100) NOT NULL,
  `remoteURI` varchar(400) DEFAULT NULL,
  `localURI` varchar(400) DEFAULT NULL,
  `result` bigint(20) DEFAULT NULL,
  `hasErrors` tinyint(1) DEFAULT NULL,
  `message` varchar(400) DEFAULT NULL,
  `date` datetime NOT NULL,
  `needToRedo` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `operacoes_financeiras` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `carteiras_investimento_id` int(11) NOT NULL,
  `cnpj_fundo_id` int(11) NOT NULL,
  `distribuidor_fundo_id` int(11) DEFAULT NULL,
  `tipo_operacoes_financeira_id` int(11) NOT NULL,
  `por_valor` tinyint(1) NOT NULL,
  `valor_total` double(24,2) NOT NULL,
  `valor_cota` double(24,2) NOT NULL,
  `quantidade_cotas` int(11) NOT NULL,
  `data` date NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `permissaos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `supor_fck_root` tinyint(1) NOT NULL,
  `administrador_mng` tinyint(1) NOT NULL,
  `carteiras_mng` tinyint(1) NOT NULL,
  `logs_mng` tinyint(1) NOT NULL,
  `operacoes_mng` tinyint(1) NOT NULL,
  `usuarios_mng` tinyint(1) NOT NULL,
  `tipos_mng` tinyint(1) NOT NULL,
  `rel_mng` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `rel_benchmarks_classe_fundos` (
  `tipo_benchmark_id` int(11) NOT NULL,
  `tipo_classe_fundo_id` int(11) NOT NULL,
  PRIMARY KEY (`tipo_benchmark_id`,`tipo_classe_fundo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `rel_carteiras_operacoes` (
  `carteiras_investimento_id` int(11) NOT NULL,
  `operacoes_financeira_id` int(11) NOT NULL,
  PRIMARY KEY (`carteiras_investimento_id`,`operacoes_financeira_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `retorno_risco_fundos` (
  `cnpj_fundo_id` int(11) NOT NULL,
  `periodo_meses` int(11) NOT NULL,
  `data_final` date NOT NULL,
  `rentab_media` decimal(24,12) NOT NULL,
  `desvio_padrao` decimal(24,12) NOT NULL,
  `num_valores` int(11) DEFAULT NULL,
  `rentab_min` decimal(24,12) DEFAULT NULL,
  `rentab_max` decimal(24,12) DEFAULT NULL,
  `meses_abaixo_bench` int(11) DEFAULT NULL,
  `meses_acima_bench` int(11) DEFAULT NULL,
  `cnpj_fundo` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`cnpj_fundo_id`,`data_final`,`periodo_meses`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `sessions` (
  `id` char(40) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `created` datetime DEFAULT current_timestamp(),
  `modified` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `data` blob DEFAULT NULL,
  `expires` int(10) UNSIGNED DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `situacao_fundos` (
  `cnpj_fundo_id` int(11) NOT NULL,
  `tipo_situacao_fundo_id` int(11) NOT NULL,
  `DT_INI_SIT` date NOT NULL,
  `DT_REG_CVM` date NOT NULL,
  PRIMARY KEY (`cnpj_fundo_id`,`tipo_situacao_fundo_id`,`DT_INI_SIT`) USING BTREE,
  KEY `FK_ID_FUNDO` (`cnpj_fundo_id`),
  KEY `FK_ID_SIT` (`tipo_situacao_fundo_id`),
  KEY `IDX_DT_INI_SIT` (`DT_INI_SIT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tipo_anbima_classes` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `classe_anbima` varchar(100) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `IDX_CLASSE_ANBIMA` (`classe_anbima`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `tipo_anbima_classes` (`id`, `classe_anbima`) VALUES
(9, ''),
(21, 'AÇÕES - ATIVO - DIVIDENDOS'),
(31, 'AÇÕES - ATIVO - ÍNDICE ATIVO'),
(3, 'AÇÕES - ATIVO - LIVRE'),
(45, 'AÇÕES - ATIVO - SETORIAIS'),
(32, 'AÇÕES - ATIVO - SMALL CAPS'),
(42, 'AÇÕES - ATIVO - SUSTENTABILIDADE / GOVERNANÇA'),
(33, 'AÇÕES - ATIVO - VALOR / CRESCIMENTO'),
(34, 'AÇÕES - FUNDOS FECHADOS'),
(28, 'AÇÕES - INDEXADO - ÍNDICE PASSIVO'),
(27, 'AÇÕES - INVESTIMENTO NO EXTERIOR'),
(24, 'AÇÕES - MONO AÇÃO'),
(18, 'FUNDO CAMBIAL'),
(11, 'MULTIMERCADO - ALOCAÇÃO - BALANCEADOS'),
(10, 'MULTIMERCADO - ALOCAÇÃO - DINÂMICOS'),
(30, 'MULTIMERCADO - ESTRATÉGIA - CAPITAL PROTEGIDO'),
(19, 'MULTIMERCADO - ESTRATÉGIA - ESTRATÉGIA ESPECÍFICA'),
(14, 'MULTIMERCADO - ESTRATÉGIA - JUROS E MOEDAS'),
(7, 'MULTIMERCADO - ESTRATÉGIA - LIVRE'),
(29, 'MULTIMERCADO - ESTRATÉGIA - LONG & SHORT DIRECIONAL'),
(22, 'MULTIMERCADO - ESTRATÉGIA - LONG & SHORT NEUTRO'),
(5, 'MULTIMERCADO - ESTRATÉGIA - MACRO'),
(36, 'MULTIMERCADO - ESTRATÉGIA - TRADING'),
(15, 'MULTIMERCADO - INVESTIMENTO NO EXTERIOR'),
(52, 'PREVIDÊNCIA - AÇÕES ATIVO'),
(69, 'PREVIDÊNCIA - AÇÕES INDEXADO'),
(56, 'PREVIDÊNCIA - BALANCEADOS - ACIMA DE 49'),
(61, 'PREVIDÊNCIA - BALANCEADOS - DATA-ALVO'),
(57, 'PREVIDÊNCIA - BALANCEADOS - DE 30-49'),
(58, 'PREVIDÊNCIA - MULTIMERCADOS JUROS E MOEDAS'),
(50, 'PREVIDÊNCIA - MULTIMERCADOS LIVRE'),
(66, 'PREVIDÊNCIA - RF DATA-ALVO'),
(65, 'PREVIDÊNCIA - RF DURAÇÃO ALTA - CRÉDITO LIVRE'),
(63, 'PREVIDÊNCIA - RF DURAÇÃO ALTA - GRAU DE INVESTIMENTO'),
(64, 'PREVIDÊNCIA - RF DURAÇÃO ALTA - SOBERANO'),
(60, 'PREVIDÊNCIA - RF DURAÇÃO BAIXA - CRÉDITO LIVRE'),
(51, 'PREVIDÊNCIA - RF DURAÇÃO BAIXA - GRAU DE INVESTIMENTO'),
(62, 'PREVIDÊNCIA - RF DURAÇÃO BAIXA - SOBERANO'),
(59, 'PREVIDÊNCIA - RF DURAÇÃO LIVRE - CRÉDITO LIVRE'),
(53, 'PREVIDÊNCIA - RF DURAÇÃO LIVRE - GRAU DE INVESTIMENTO'),
(55, 'PREVIDÊNCIA - RF DURAÇÃO LIVRE - SOBERANO'),
(67, 'PREVIDÊNCIA - RF DURAÇÃO MÉDIA - CRÉDITO LIVRE'),
(49, 'PREVIDÊNCIA - RF DURAÇÃO MÉDIA - GRAU DE INVESTIMENTO'),
(68, 'PREVIDÊNCIA - RF DURAÇÃO MÉDIA - SOBERANO'),
(54, 'PREVIDÊNCIA - RF INDEXADOS'),
(47, 'PREVIDÊNCIA AÇÕES'),
(41, 'PREVIDÊNCIA BALANCEADOS - ACIMA DE 30'),
(39, 'PREVIDÊNCIA BALANCEADOS - ATÉ 15'),
(38, 'PREVIDÊNCIA BALANCEADOS - DE 15 A 30'),
(44, 'PREVIDÊNCIA DATA ALVO (FIQ)'),
(40, 'PREVIDÊNCIA MULTIMERCADO'),
(37, 'RENDA FIXA'),
(46, 'RENDA FIXA - INV. NO EXTERIOR'),
(48, 'RENDA FIXA - INV. NO EXTERIOR - DÍVIDA EXTERNA'),
(25, 'RENDA FIXA - PASSIVO - ÍNDICES'),
(43, 'RENDA FIXA ALTA DURAÇÃO - CRÉDITO LIVRE'),
(35, 'RENDA FIXA ALTA DURAÇÃO - GRAU DE INVESTIMENTO'),
(23, 'RENDA FIXA ALTA DURAÇÃO - SOBERANO'),
(13, 'RENDA FIXA BAIXA DURAÇÃO - CRÉDITO LIVRE'),
(12, 'RENDA FIXA BAIXA DURAÇÃO - GRAU DE INVESTIMENTO'),
(4, 'RENDA FIXA BAIXA DURAÇÃO - SOBERANO'),
(26, 'RENDA FIXA LIVRE DURAÇÃO - CRÉDITO LIVRE'),
(2, 'RENDA FIXA LIVRE DURAÇÃO - GRAU DE INVESTIMENTO'),
(6, 'RENDA FIXA LIVRE DURAÇÃO - SOBERANO'),
(20, 'RENDA FIXA MÉDIA DURAÇÃO - CRÉDITO LIVRE'),
(8, 'RENDA FIXA MÉDIA DURAÇÃO - GRAU DE INVESTIMENTO'),
(16, 'RENDA FIXA MÉDIA DURAÇÃO - SOBERANO'),
(17, 'RENDA FIXA SIMPLES');

CREATE TABLE IF NOT EXISTS `tipo_benchmarks` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nome` varchar(400) NOT NULL,
  `sigla` varchar(8) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_NOME` (`nome`),
  UNIQUE KEY `IDX_SIGLA` (`sigla`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `tipo_benchmarks` (`id`, `nome`, `sigla`) VALUES
(1, 'Certificado de Depósito Interbancário', 'CDI'),
(2, 'benchmarks para títulos públicos (família IMA)', 'IMA-G');

CREATE TABLE IF NOT EXISTS `tipo_classe_fundos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `classe` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `tipo_classe_fundos` (`id`, `classe`) VALUES
(1, 'Fundo de Renda Fixa'),
(2, 'Fundo Multimercado'),
(3, 'Fundo de Ações'),
(4, 'Fundo da Dívida Externa'),
(5, 'Fundo Referenciado'),
(6, 'Fundo de Curto Prazo'),
(7, 'Fundo Cambial'),
(8, '');

CREATE TABLE IF NOT EXISTS `tipo_etapas_registros` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nome_curto` varchar(16) NOT NULL,
  `etapa` varchar(100) NOT NULL,
  `podeLogar` tinyint(1) NOT NULL,
  `podeInvestir` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `tipo_etapas_registros` (`id`, `nome_curto`, `etapa`, `podeLogar`, `podeInvestir`) VALUES
(1, 'FormBasico', 'Preechimento do formulário básico', 0, 0),
(2, 'EnvioMsg', 'Envio de mensagem para validação de e-mail', 0, 0),
(3, 'ValidacaoEmail', 'Validação de e-mail concluída', 1, 0),
(4, 'FormCompleto', 'Preechimento do formulário completo', 1, 0),
(5, 'PerfilInvest', 'Preenchimento do perfil do investidor', 1, 1);

CREATE TABLE IF NOT EXISTS `tipo_operacoes_financeiras` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nome` varchar(100) NOT NULL,
  `is_aplicacao` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `tipo_operacoes_financeiras` (`id`, `nome`, `is_aplicacao`) VALUES
(2, 'Aplicação', 1),
(3, 'Resgate parcial', 0),
(4, 'Resgate total', 0),
(5, 'Conciliação de saldo (resgate)', 0),
(6, 'Taxas ou emolumentos', 0),
(7, 'Impostos', 0);

CREATE TABLE IF NOT EXISTS `tipo_participantes` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nome` varchar(50) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tipo_planos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nome` varchar(40) NOT NULL,
  `descricao` varchar(200) NOT NULL,
  `permissao_id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `tipo_planos` (`id`, `nome`, `descricao`, `permissao_id`) VALUES
(1, 'FI Básico', 'Plano básico com análise de fundos de investimentos contendo dois indicadores', 1),
(2, 'FI Básico plus', 'Plano básico com análise de fundos de investimentos contendo vários indicadores e gráficos', 1),
(3, 'FI+CA Básico', 'Plano básico com análise de fundos de investimentos e companhias abertas, contendo indicadores essenciais', 1),
(4, 'root', 'just the root', 2);

CREATE TABLE IF NOT EXISTS `tipo_rentabilidade_fundos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `rentabilidade` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `tipo_rentabilidade_fundos` (`id`, `rentabilidade`) VALUES
(1, 'OUTROS'),
(2, 'DI de um dia'),
(3, ''),
(4, 'Ibovespa'),
(5, 'Índice de Preços ao Consumidor Amplo (IPCA/IBGE)'),
(6, 'IBrX'),
(7, 'Taxa Selic'),
(8, 'Índice de Mercado Andima Geral'),
(9, 'Índice Geral de Preços-Mercado (IGP-M)'),
(10, 'IRF-M'),
(11, 'Taxa de juro prefixada'),
(12, 'Dólar comercial'),
(13, 'Índice de Mercado Andima NTN-B até 5 anos'),
(14, 'Índice de Mercado Andima NTN-B mais de 5 anos'),
(15, 'Índice Nacional de Preços ao Consumidor (INPC/IBGE)'),
(16, 'Euro'),
(17, 'IBrX-50'),
(18, 'Cota de PIBB'),
(19, 'Índice de Mercado Andima todas NTN-B'),
(20, 'Índice de Preços ao Consumidor (IPC/FIPE)'),
(21, 'Índice de preços'),
(22, 'Índice de Mercado Andima LFT'),
(23, 'Taxa de Juro de Longo Prazo'),
(24, 'Taxa Referencial'),
(25, 'Taxa Anbid'),
(26, 'Índice Geral de Preços-Disponibilidade Interna (IGP-DI)'),
(27, 'IEE'),
(28, 'Índice de Mercado Andima todas NTN-C'),
(29, 'Ouro 250 gramas'),
(30, 'Índice de Mercado Andima NTN-C até 5 anos'),
(31, 'Taxa Básica Financeira');

CREATE TABLE IF NOT EXISTS `tipo_setor_ativ_cias_abertas` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `setor_ativ` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_setor_ativ` (`setor_ativ`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `tipo_setor_ativ_cias_abertas` (`id`, `setor_ativ`) VALUES
(45, ''),
(14, 'Agricultura (Açúcar, Álcool e Cana)'),
(33, 'Alimentos'),
(9, 'Arrendamento Mercantil'),
(28, 'Bancos'),
(29, 'Bebidas e Fumo'),
(42, 'Bolsas de Valores/Mercadorias e Futuros'),
(36, 'Brinquedos e Lazer'),
(8, 'Comércio (Atacado e Varejo)'),
(50, 'Comércio Exterior'),
(48, 'Comunicação e Informática'),
(1, 'Construção Civil, Mat. Constr. e Decoração'),
(43, 'Crédito Imobiliário'),
(35, 'Educação'),
(46, 'Embalagens'),
(60, 'Emp. Adm. Part. - Agricultura (Açúcar, Álcool e Cana)'),
(53, 'Emp. Adm. Part. - Alimentos'),
(62, 'Emp. Adm. Part. - Arrendamento Mercantil'),
(51, 'Emp. Adm. Part. - Bancos'),
(69, 'Emp. Adm. Part. - Brinquedos e Lazer'),
(25, 'Emp. Adm. Part. - Comércio (Atacado e Varejo)'),
(41, 'Emp. Adm. Part. - Comunicação e Informática'),
(11, 'Emp. Adm. Part. - Const. Civil, Mat. Const. e Decoração'),
(47, 'Emp. Adm. Part. - Crédito Imobiliário'),
(34, 'Emp. Adm. Part. - Educação'),
(64, 'Emp. Adm. Part. - Embalagens'),
(4, 'Emp. Adm. Part. - Energia Elétrica'),
(52, 'Emp. Adm. Part. - Extração Mineral'),
(66, 'Emp. Adm. Part. - Farmacêutico e Higiene'),
(67, 'Emp. Adm. Part. - Gráficas e Editoras'),
(49, 'Emp. Adm. Part. - Hospedagem e Turismo'),
(22, 'Emp. Adm. Part. - Intermediação Financeira'),
(56, 'Emp. Adm. Part. - Máqs., Equip., Veíc. e Peças'),
(57, 'Emp. Adm. Part. - Metalurgia e Siderurgia'),
(70, 'Emp. Adm. Part. - Papel e Celulose'),
(58, 'Emp. Adm. Part. - Petróleo e Gás'),
(61, 'Emp. Adm. Part. - Petroquímicos e Borracha'),
(54, 'Emp. Adm. Part. - Saneamento, Serv. Água e Gás'),
(65, 'Emp. Adm. Part. - Securitização de Recebíveis'),
(27, 'Emp. Adm. Part. - Seguradoras e Corretoras'),
(5, 'Emp. Adm. Part. - Sem Setor Principal'),
(40, 'Emp. Adm. Part. - Serviços médicos'),
(20, 'Emp. Adm. Part. - Serviços Transporte e Logística'),
(39, 'Emp. Adm. Part. - Telecomunicações'),
(32, 'Emp. Adm. Part. - Têxtil e Vestuário'),
(55, 'Emp. Adm. Part.-Bolsas de Valores/Mercadorias e Futuros'),
(6, 'Emp. Adm. Participações'),
(18, 'Energia Elétrica'),
(38, 'Extração Mineral'),
(68, 'Factoring'),
(15, 'Farmacêutico e Higiene'),
(10, 'Gráficas e Editoras'),
(30, 'Hospedagem e Turismo'),
(44, 'Intermediação Financeira'),
(7, 'Máquinas, Equipamentos, Veículos e Peças'),
(12, 'Metalurgia e Siderurgia'),
(21, 'Outras Atividades Industriais'),
(37, 'Papel e Celulose'),
(59, 'Pesca'),
(3, 'Petróleo e Gás'),
(13, 'Petroquímicos e Borracha'),
(63, 'Reflorestamento'),
(16, 'Saneamento, Serv. Água e Gás'),
(2, 'Securitização de Recebíveis'),
(19, 'Seguradoras e Corretoras'),
(17, 'Serviços Diversos'),
(71, 'Serviços em Geral'),
(31, 'Serviços Médicos'),
(26, 'Serviços Transporte e Logística'),
(24, 'Telecomunicações'),
(23, 'Têxtil e Vestuário');

CREATE TABLE IF NOT EXISTS `tipo_situacao_fundos` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `situacao` varchar(40) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `tipo_situacao_fundos` (`id`, `situacao`) VALUES
(1, 'EM FUNCIONAMENTO NORMAL'),
(2, 'CANCELADA'),
(3, 'FASE PRÉ-OPERACIONAL'),
(4, ''),
(5, 'LIQUIDAÇÃO'),
(6, 'EM LIQUIDAÇÃO ORDINÁRIA');

CREATE TABLE IF NOT EXISTS `usuarios` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cpf` varchar(11) NOT NULL,
  `nome` varchar(200) NOT NULL,
  `email` varchar(100) NOT NULL,
  `dt_nasc` date NOT NULL,
  `senha` char(64) NOT NULL,
  `dt_reg` date NOT NULL,
  `tipo_plano_id` int(11) NOT NULL,
  `tipo_etapas_registro_id` mediumint(11) NOT NULL,
  `coment` varchar(100) DEFAULT NULL,
  `created` datetime NOT NULL DEFAULT current_timestamp(),
  `modified` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `UNIQUE_VALS` (`cpf`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `usuarios_aguardando_validacoes_emails` (
  `usuarios_id` int(11) NOT NULL,
  `codigo_validacao` varchar(32) NOT NULL,
  `data_envio_email` datetime NOT NULL,
  `num_envios` int(11) NOT NULL DEFAULT 1,
  PRIMARY KEY (`usuarios_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


ALTER TABLE `indicadores_fundos`
  ADD CONSTRAINT `cnpj_fundo_id` FOREIGN KEY (`cnpj_fundo_id`) REFERENCES `cnpj_fundos` (`id`);

ALTER TABLE `situacao_fundos`
  ADD CONSTRAINT `FK_ID_FUNDO` FOREIGN KEY (`cnpj_fundo_id`) REFERENCES `cnpj_fundos` (`id`),
  ADD CONSTRAINT `FK_ID_SIT` FOREIGN KEY (`tipo_situacao_fundo_id`) REFERENCES `tipo_situacao_fundos` (`ID`);
SET FOREIGN_KEY_CHECKS=1;

