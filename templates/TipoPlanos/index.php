<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoPlano[]|\Cake\Collection\CollectionInterface $tipoPlanos
 */
?>
<div class="tipoPlanos index content">
    <?= $this->Html->link(__('New Tipo Plano'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Tipo Planos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('nome') ?></th>
                    <th><?= $this->Paginator->sort('descricao') ?></th>
                    <th><?= $this->Paginator->sort('permissao_id') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($tipoPlanos as $tipoPlano): ?>
                <tr>
                    <td><?= $this->Number->format($tipoPlano->id) ?></td>
                    <td><?= h($tipoPlano->nome) ?></td>
                    <td><?= h($tipoPlano->descricao) ?></td>
                    <td><?= $tipoPlano->has('permissao') ? $this->Html->link($tipoPlano->permissao->id, ['controller' => 'Permissaos', 'action' => 'view', $tipoPlano->permissao->id]) : '' ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $tipoPlano->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $tipoPlano->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $tipoPlano->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoPlano->id)]) ?>
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
