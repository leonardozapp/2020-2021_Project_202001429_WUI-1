<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\Usuario[]|\Cake\Collection\CollectionInterface $usuarios
 */
?>
<div class="usuarios index content">
    <?= $this->Html->link(__('New Usuario'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Usuarios') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('cpf') ?></th>
                    <th><?= $this->Paginator->sort('nome') ?></th>
                    <th><?= $this->Paginator->sort('email') ?></th>
                    <th><?= $this->Paginator->sort('dt_nasc') ?></th>
                    <th><?= $this->Paginator->sort('senha') ?></th>
                    <th><?= $this->Paginator->sort('dt_reg') ?></th>
                    <th><?= $this->Paginator->sort('tipo_plano_id') ?></th>
                    <th><?= $this->Paginator->sort('tipo_etapas_registro_id') ?></th>
                    <th><?= $this->Paginator->sort('coment') ?></th>
                    <th><?= $this->Paginator->sort('created') ?></th>
                    <th><?= $this->Paginator->sort('modified') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($usuarios as $usuario): ?>
                <tr>
                    <td><?= $this->Number->format($usuario->id) ?></td>
                    <td><?= h($usuario->cpf) ?></td>
                    <td><?= h($usuario->nome) ?></td>
                    <td><?= h($usuario->email) ?></td>
                    <td><?= h($usuario->dt_nasc) ?></td>
                    <td><?= h($usuario->senha) ?></td>
                    <td><?= h($usuario->dt_reg) ?></td>
                    <td><?= $usuario->has('tipo_plano') ? $this->Html->link($usuario->tipo_plano->nome, ['controller' => 'TipoPlanos', 'action' => 'view', $usuario->tipo_plano->id]) : '' ?></td>
                    <td><?= $usuario->has('tipo_etapas_registro') ? $this->Html->link($usuario->tipo_etapas_registro->id, ['controller' => 'TipoEtapasRegistros', 'action' => 'view', $usuario->tipo_etapas_registro->id]) : '' ?></td>
                    <td><?= h($usuario->coment) ?></td>
                    <td><?= h($usuario->created) ?></td>
                    <td><?= h($usuario->modified) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $usuario->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $usuario->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $usuario->id], ['confirm' => __('Are you sure you want to delete # {0}?', $usuario->id)]) ?>
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
