<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\CnpjFundo[]|\Cake\Collection\CollectionInterface $cnpjFundos
 */
?>
<div class="cnpjFundos index content">
    <?= $this->Html->link(__('New Cnpj Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Cnpj Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('CNPJ') ?></th>
                    <th><?= $this->Paginator->sort('DENOM_SOCIAL') ?></th>
                    <th><?= $this->Paginator->sort('DT_REG_CVM') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($cnpjFundos as $cnpjFundo): ?>
                <tr>
                    <td><?= $this->Number->format($cnpjFundo->id) ?></td>
                    <td><?= h($cnpjFundo->CNPJ) ?></td>
                    <td><?= h($cnpjFundo->DENOM_SOCIAL) ?></td>
                    <td><?= h($cnpjFundo->DT_REG_CVM) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $cnpjFundo->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $cnpjFundo->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $cnpjFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $cnpjFundo->id)]) ?>
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
