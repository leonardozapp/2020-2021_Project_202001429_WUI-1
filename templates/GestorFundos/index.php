<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\GestorFundo[]|\Cake\Collection\CollectionInterface $gestorFundos
 */
?>
<div class="gestorFundos index content">
    <?= $this->Html->link(__('New Gestor Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Gestor Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('cnpj') ?></th>
                    <th><?= $this->Paginator->sort('nome') ?></th>
                    <th><?= $this->Paginator->sort('DT_REG_CVM') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($gestorFundos as $gestorFundo): ?>
                <tr>
                    <td><?= $this->Number->format($gestorFundo->id) ?></td>
                    <td><?= h($gestorFundo->cnpj) ?></td>
                    <td><?= h($gestorFundo->nome) ?></td>
                    <td><?= h($gestorFundo->DT_REG_CVM) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $gestorFundo->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $gestorFundo->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $gestorFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $gestorFundo->id)]) ?>
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
