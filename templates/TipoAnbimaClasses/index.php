<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoAnbimaClass[]|\Cake\Collection\CollectionInterface $tipoAnbimaClasses
 */
?>
<div class="tipoAnbimaClasses index content">
    <?= $this->Html->link(__('New Tipo Anbima Class'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Tipo Anbima Classes') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('classe_anbima') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($tipoAnbimaClasses as $tipoAnbimaClass): ?>
                <tr>
                    <td><?= $this->Number->format($tipoAnbimaClass->id) ?></td>
                    <td><?= h($tipoAnbimaClass->classe_anbima) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $tipoAnbimaClass->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $tipoAnbimaClass->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $tipoAnbimaClass->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoAnbimaClass->id)]) ?>
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
        <p><?= $this->Paginator->counter(__('Página {{page}} de {{pages}}, mostrando {{current}} registro(s) do total de {{count}}')) ?></p>
    </div>
</div>
