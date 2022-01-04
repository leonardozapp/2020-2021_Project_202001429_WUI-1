<nav class="top-nav">
	<div class="top-nav-title">
		<a href="/">
			<!--img src="img/logo.jpeg"  alt="" width="64" height="48"-->
			<?php echo $this->element('nomeApp'); ?>
		</a>
	</div>

	<?= $this->element('mainMenu') ?>

	<div class="top-nav-links">
		<?php
		$session = $this->request->getSession();
		$conectado = $session->read('User.id') != null;
		if ($conectado) {
			$firstname = $session->read('User.firstname');
		?>
			<div class="dropdown">
				<button class="dropbtn">Olá, <?= $firstname ?></button>
				<div class="dropdown-content">
					<a href='/Usuarios/view/<?= $session->read('User.id') ?>'>Conta e dados pessoais</a>
					<a href="/Usuarios/perfilInvestidor">Perfil do investidor</a>
					<a href="/Usuarios/logout">Sair</a>
				</div>
			</div>
		<?php } else { ?>
			<div class="dropdown">
				<button class="dropbtn">Entrar</button>
				<div class="dropdown-content">
					<a href="/Usuarios/login">Login</a>
					<a href="/Usuarios/register">Registrar</a>
				</div>
			</div>
		<?php } ?>
	</div>
</nav>