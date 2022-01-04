<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoOperacoesFinanceira[]|\Cake\Collection\CollectionInterface $tipoOperacoesFinanceiras
 */
?>
<div class="tipoOperacoesFinanceiras index content">
    <?= $this->Html->link(__('New Tipo Operacoes Financeira'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Tipo Operacoes Financeiras') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('nome') ?></th>
                    <th><?= $this->Paginator->sort('is_aplicacao') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($tipoOperacoesFinanceiras as $tipoOperacoesFinanceira): ?>
                <tr>
                    <td><?= $this->Number->format($tipoOperacoesFinanceira->id) ?></td>
                    <td><?= h($tipoOperacoesFinanceira->nome) ?></td>
                    <td><?= h($tipoOperacoesFinanceira->is_aplicacao) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $tipoOperacoesFinanceira->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $tipoOperacoesFinanceira->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $tipoOperacoesFinanceira->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoOperacoesFinanceira->id)]) ?>
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
