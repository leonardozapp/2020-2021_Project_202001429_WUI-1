<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoSituacaoFundo[]|\Cake\Collection\CollectionInterface $tipoSituacaoFundos
 */
?>
<div class="tipoSituacaoFundos index content">
    <?= $this->Html->link(__('New Tipo Situacao Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Tipo Situacao Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('situacao') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($tipoSituacaoFundos as $tipoSituacaoFundo): ?>
                <tr>
                    <td><?= $this->Number->format($tipoSituacaoFundo->id) ?></td>
                    <td><?= h($tipoSituacaoFundo->situacao) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $tipoSituacaoFundo->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $tipoSituacaoFundo->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $tipoSituacaoFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoSituacaoFundo->id)]) ?>
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
