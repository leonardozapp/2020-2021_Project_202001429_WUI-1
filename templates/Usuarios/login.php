<?php
/**
 * @var \App\View\AppView $this
 */
?>
<div class="usuarios form content">
	<h2><?=__("Login")?></h2>
	<?= $this->Form->create() ?>
    <fieldset>
        <!--legend><?= __('Please enter your username and password') ?></legend-->
		<?= $this->Form->control('username',['label' => __('CPF (apenas números)')]) ?>
		<?= $this->Form->control('password',['label' => __('Senha')]) ?>
    </fieldset>
	<?= $this->Form->button(__('Entrar')); ?>
	<?= $this->Form->end() ?>
</div>
<div class="row">
	<a href="">Esqueci/Recuperar minha senha</a>
</div>