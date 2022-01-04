<!DOCTYPE html>
<html>
    <head>
		<?= $this->Html->charset() ?>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>
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
		<?= $this->Html->script("http://ajax.googleapis.com/ajax/libs/jquery/1.11.0/jquery.min.js"); ?>
		<?= $this->Html->script("http://ajax.googleapis.com/ajax/libs/jqueryui/1.10.4/jquery-ui.min.js") ?>
		<script src="https://code.jquery.com/jquery-3.5.1.js"></script>
    </head>

    <body>
		<?php echo$this->element('topNavBar'); ?>
        <main class="main">
            <div class="container">
                <div class="content">
					<?= $this->Flash->render() ?>
					<?= $this->fetch('content') ?>
                </div>
            </div>
        </main>
		<?php echo$this->element('footer'); ?>
    </body>
</html>
