<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoClasseFundo[]|\Cake\Collection\CollectionInterface $tipoClasseFundos
 */
?>
<div class="tipoClasseFundos index content">
    <?= $this->Html->link(__('New Tipo Classe Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Tipo Classe Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('classe') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($tipoClasseFundos as $tipoClasseFundo): ?>
                <tr>
                    <td><?= $this->Number->format($tipoClasseFundo->id) ?></td>
                    <td><?= h($tipoClasseFundo->classe) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $tipoClasseFundo->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $tipoClasseFundo->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $tipoClasseFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoClasseFundo->id)]) ?>
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
