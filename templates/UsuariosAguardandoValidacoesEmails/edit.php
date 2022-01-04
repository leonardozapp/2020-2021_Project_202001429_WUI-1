<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\UsuariosAguardandoValidacoesEmail $usuariosAguardandoValidacoesEmail
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $usuariosAguardandoValidacoesEmail->usuarios_id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $usuariosAguardandoValidacoesEmail->usuarios_id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Usuarios Aguardando Validacoes Emails'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="usuariosAguardandoValidacoesEmails form content">
            <?= $this->Form->create($usuariosAguardandoValidacoesEmail) ?>
            <fieldset>
                <legend><?= __('Edit Usuarios Aguardando Validacoes Email') ?></legend>
                <?php
                    echo $this->Form->control('codigo_validacao');
                    echo $this->Form->control('data_envio_email');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
