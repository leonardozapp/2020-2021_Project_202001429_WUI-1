<?php

use Cake\Core\Configure;
use Cake\Http\Exception\NotFoundException;

$this->disableAutoLayout();
$cakeDescription = 'FundosInvest';
$session = $this->request->getSession();
$conectado = $session->read('User.id') != null;
?>

<!DOCTYPE html>
<html>
    <head>
		<?= $this->Html->charset() ?>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>
			<?= $cakeDescription ?>:
			<?= $this->fetch('title') ?>
        </title>
		<?= $this->Html->meta('icon') ?>
        <link href="https://fonts.googleapis.com/css?family=Raleway:400,700" rel="stylesheet">
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/normalize.css@8.0.1/normalize.css">
		<?= $this->Html->css('milligram.min.css') ?>
		<?= $this->Html->css('cake.css') ?>
		<?= $this->Html->css('menus.css') ?>
		<?= $this->fetch('meta') ?>
		<?= $this->fetch('css') ?>
		<?= $this->fetch('script') ?>
    </head>

    <body>
		<?= $this->element('topNavBar') ?>
        <header>
			<div class="container text-center">
				<?php if (!$conectado) { ?>
					<h1 class="top-nav-title">Bem-vindo ao <?= $this->element('nomeApp', ['showVersion' => true]) ?></h1>
				<?php } else { ?>
					<h1 class="top-nav-title">Bem-vindo de volta ao <?= $this->element('nomeApp', ['showVersion' => true]) ?>, <?= $session->read('User.firstname') ?></h1>
				<?php } ?>
				<h3>Serviço web para auxílio à escolha de fundos de investimento para investidores inexperientes</h3>
				<?= $this->Flash->render() ?>
			</div>
		</header>
        <main class="main">
            <div class="container">
                <div class="content">
					<?= $this->element('home_texto_intro') ?>

					<?php
					if (!$conectado) {
						?>
						<br/>
						<strong>
							<a href="/Usuarios/login">Entrar</a>
							&nbsp;&nbsp;&nbsp;&nbsp;						
							<a href="/Usuarios/register">Registrar</a><br/>							
						</strong>
					<?php } ?>
					<hr>
					<?= $this->element('avisoSistemaExperimental') ?>
					<hr>
					<?= $this->element('servicos_disponiveis') ?>
                </div>
            </div>
        </div>
    </main>
	<?= $this->element('footer') ?>    
	<ul id=menu>
		<li><a href="">Precisa de Ajuda?</a>
	</ul>

</body>
</html>
