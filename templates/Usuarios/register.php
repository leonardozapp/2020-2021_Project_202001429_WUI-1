<div class="column-responsive column-80">
    <div class="usuarios form content">
		<?= $this->Form->create($usuario) ?>
        <fieldset>
            <legend><?= __('Registrar novo usuário') ?></legend>
            <div>
                Para registrar-se no <?php echo$this->element('nomeApp'); ?> preencha todos os campos abaixo, leia os Termos e Condições de Uso, e então pressione o botão "Registrar". 
            </div>
            <hr>
			<?php
			//$this->Form->horizontal = false;
			echo $this->Form->control('cpf', ['label' => __('CPF (apenas números)')]);
			echo $this->Form->control('nome', ['label' => __('Nome Completo (conforme consta no documento)')]);
			echo $this->Form->control('email', ['label' => __('Seu melhor e-mail'), 'type' => 'email']);
			echo $this->Form->control('dt_nasc', ['label' => __('Data de Nascimento'), 'type' => 'date']);
			echo $this->Form->control('senha', ['label' => __('Senha'), 'type' => 'password']);
			echo $this->Form->control('senha2', ['label' => __('Repita a senha'), 'type' => 'password']);
			echo $this->element('termosUso');
			echo $this->Form->control('concordaTermos', ['label' => __('Li e concordo com os termos e condições de uso'), 'type' => 'checkbox']);
			?>
        </fieldset>
        <hr>
		<?= $this->Form->button(__('Registrar'), ['type' => 'submit']) ?>
		<?= $this->Form->end() ?>
    </div>

</div>