<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\AdministradorFundo[]|\Cake\Collection\CollectionInterface $administradorFundos
 */
?>
<div class="administradorFundos index content">
    <?= $this->Html->link(__('New Administrador Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Administrador Fundos') ?></h3>
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
                <?php foreach ($administradorFundos as $administradorFundo): ?>
                <tr>
                    <td><?= $this->Number->format($administradorFundo->id) ?></td>
                    <td><?= h($administradorFundo->cnpj) ?></td>
                    <td><?= h($administradorFundo->nome) ?></td>
                    <td><?= h($administradorFundo->DT_REG_CVM) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $administradorFundo->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $administradorFundo->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $administradorFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $administradorFundo->id)]) ?>
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
