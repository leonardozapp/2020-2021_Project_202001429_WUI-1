<?php
$session = $this->request->getSession();
$conectado = $session->read('User.id') != null;
$firstname = $session->read('User.firstname');
$administrador = $conectado && $session->read('User.plano') == 4;
?>

<div class="top-nav-links">
	<div class="dropdown">
		<button class="dropbtn"><?= __('Educação financeira') ?></button>
		<div class="dropdown-content">
			<a href="/Educacao/index"><?= __('Comece por aqui') ?></a>
			<a href="/Educacao/index"><?= __('Informações gerais') ?></a>
			<a href="/Educacao/index"><?= __('Tipos de investimentos') ?></a>
			<a href="/Educacao/index"><?= __('Como começar a investir') ?></a>
			<a href="/Educacao/index"><?= __('Perfis de investidores') ?></a>
		</div>
	</div> 
	<div class="dropdown">
		<button class="dropbtn"><?= __('Fundos de investimento') ?></button>
		<div class="dropdown-content">
			<!--a href="/Fundos/index"><?= __('Busca geral Cnpj') ?></a-->
			<a href="/Fundos/busca"><?= __('Busca geral') ?></a>
			<!--<a href="/Fundos/indicadores"><?= __('Indicadores financeiros') ?></a>-->
			<a href="/Fundos/comparacao"><?= __('Comparação de fundos') ?></a>
		</div>
	</div> 
	<div class="dropdown">
		<button class="dropbtn"><?= __('Empresas abertas') ?></button>
		<div class="dropdown-content">
			<?php if ($conectado) { ?>
				<a href="/Acoes/busca"><?= __('Busca geral') ?></a>
				<!--<a href="/Acoes/indicfund"><?= __('Indicadores fundamentalistas') ?></a>
				<a href="/Acoes/indicgraf"><?= __('Indicadores gráficos') ?></a>-->
				<a href="/Acoes/comparacao"><?= __('Comparação de ações') ?></a>
			<?php } else { ?>
				<span><?= __('Busca geral') ?></span>
				<!--<span><?= __('Indicadores fundamentalistas') ?></span>
				<span><?= __('Indicadores gráficos') ?></span>-->
				<span><?= __('Comparação de ações') ?></span>
			<?php } ?>
		</div>
	</div> 
	<div class="dropdown">
		<button class="dropbtn"><?= __('Carteira de investimento') ?></button>
		<div class="dropdown-content">
			<?php if ($conectado) { ?>
				<a href="/CarteirasInvestimentos/index"><?= __('Carteiras e operações') ?></a>
				<a href="/Portfolio/analise"><?= __('Análise da carteira') ?></a>
				<a href="/Portfolio/comparacao"><?= __('Comparação de carteiras') ?></a>
				<a href="/Portfolio/fronteira"><?= __('Fronteira eficiente') ?></a>
			<?php } else { ?>
				<span><?= __('Carteiras e operações') ?></span>
				<!--<a href="Portfolio/registro">Registro de transações</a> -->
				<span><?= __('Análise da carteira') ?></span>
				<span><?= __('Comparação de carteiras') ?></span>
				<span><?= __('Fronteira eficiente') ?></span>
			<?php } ?>
		</div>
	</div> 

	<?php if ($administrador) { ?>
		<div class="dropdown">
			<button class="dropbtn"><?= __('Administração') ?></button>
			<div class="dropdown-content">
				<a href="/Admin/logs_atualizacao"><?= __('Logs de atualização') ?></a>
				<a href="/Admin/usuarios"><?= __('Usuário') ?>)</a>
				<a href="/Admin/permissoes"><?= __('Permissões') ?></a>
				<a href="/Admin/fronteira"><?= __('Registros') ?></a>
			</div>
		</div> 
	<?php } ?>
</div>