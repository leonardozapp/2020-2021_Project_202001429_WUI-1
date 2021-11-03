CREATE TABLE IF NOT EXISTS `tipo_perfil_investidor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nome` varchar(40) NOT NULL,
  `pontuacao_minima` int(11) NOT NULL,
  `pontuacao_maxima` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO tipo_perfil_investidor (id,nome,pontuacao_minima,pontuacao_maxima)
	VALUES (1,'Conservador',0,4);
INSERT INTO tipo_perfil_investidor (id,nome,pontuacao_minima,pontuacao_maxima)
	VALUES (2,'Moderado',5,24);
INSERT INTO tipo_perfil_investidor (id,nome,pontuacao_minima,pontuacao_maxima)
	VALUES (3,'Agressivo',25,100);

ALTER TABLE usuarios ADD tipo_perfil_investidor_id int(11) NULL;
ALTER TABLE usuarios ADD CONSTRAINT usuarios_tipo_perfil_investidor_FK FOREIGN KEY (tipo_perfil_investidor_id) REFERENCES tipo_perfil_investidor(id);
