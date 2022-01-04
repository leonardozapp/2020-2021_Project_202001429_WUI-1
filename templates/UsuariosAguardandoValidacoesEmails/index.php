<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\UsuariosAguardandoValidacoesEmail[]|\Cake\Collection\CollectionInterface $usuariosAguardandoValidacoesEmails
 */
?>
<div class="usuariosAguardandoValidacoesEmails index content">
    <?= $this->Html->link(__('New Usuarios Aguardando Validacoes Email'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Usuarios Aguardando Validacoes Emails') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('usuarios_id') ?></th>
                    <th><?= $this->Paginator->sort('codigo_validacao') ?></th>
                    <th><?= $this->Paginator->sort('data_envio_email') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($usuariosAguardandoValidacoesEmails as $usuariosAguardandoValidacoesEmail): ?>
                <tr>
                    <td><?= $this->Number->format($usuariosAguardandoValidacoesEmail->usuarios_id) ?></td>
                    <td><?= h($usuariosAguardandoValidacoesEmail->codigo_validacao) ?></td>
                    <td><?= h($usuariosAguardandoValidacoesEmail->data_envio_email) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $usuariosAguardandoValidacoesEmail->usuarios_id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $usuariosAguardandoValidacoesEmail->usuarios_id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $usuariosAguardandoValidacoesEmail->usuarios_id], ['confirm' => __('Are you sure you want to delete # {0}?', $usuariosAguardandoValidacoesEmail->usuarios_id)]) ?>
                    </td>
                </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
    <div class="paginator">
        <ul class="pagination">
            <?= $this->Paginator->first('<< ' . __('first')) ?>
            <?= $this->Paginator->prev('< ' . __('previous')) ?>
            <?= $this->Paginator->numbers() ?>
            <?= $this->Paginator->next(__('next') . ' >') ?>
            <?= $this->Paginator->last(__('last') . ' >>') ?>
        </ul>
        <p><?= $this->Paginator->counter(__('Page {{page}} of {{pages}}, showing {{current}} record(s) out of {{count}} total')) ?></p>
    </div>
</div>
