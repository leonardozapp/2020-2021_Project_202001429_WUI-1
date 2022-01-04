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
            <?= $this->Html->link(__('Edit Usuarios Aguardando Validacoes Email'), ['action' => 'edit', $usuariosAguardandoValidacoesEmail->usuarios_id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Usuarios Aguardando Validacoes Email'), ['action' => 'delete', $usuariosAguardandoValidacoesEmail->usuarios_id], ['confirm' => __('Are you sure you want to delete # {0}?', $usuariosAguardandoValidacoesEmail->usuarios_id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Usuarios Aguardando Validacoes Emails'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Usuarios Aguardando Validacoes Email'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="usuariosAguardandoValidacoesEmails view content">
            <h3><?= h($usuariosAguardandoValidacoesEmail->usuarios_id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Codigo Validacao') ?></th>
                    <td><?= h($usuariosAguardandoValidacoesEmail->codigo_validacao) ?></td>
                </tr>
                <tr>
                    <th><?= __('Usuarios Id') ?></th>
                    <td><?= $this->Number->format($usuariosAguardandoValidacoesEmail->usuarios_id) ?></td>
                </tr>
                <tr>
                    <th><?= __('Data Envio Email') ?></th>
                    <td><?= h($usuariosAguardandoValidacoesEmail->data_envio_email) ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
