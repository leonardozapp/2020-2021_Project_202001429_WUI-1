<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\SituacaoFundo[]|\Cake\Collection\CollectionInterface $situacaoFundos
 */
?>
<div class="situacaoFundos index content">
    <?= $this->Html->link(__('New Situacao Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Situacao Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('cnpj_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('tipo_situacao_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('DT_INI_SIT') ?></th>
                    <th><?= $this->Paginator->sort('DT_REG_CVM') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($situacaoFundos as $situacaoFundo): ?>
                <tr>
                    <td><?= $situacaoFundo->has('cnpj_fundo') ? $this->Html->link($situacaoFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $situacaoFundo->cnpj_fundo->id]) : '' ?></td>
                    <td><?= $situacaoFundo->has('tipo_situacao_fundo') ? $this->Html->link($situacaoFundo->tipo_situacao_fundo->id, ['controller' => 'TipoSituacaoFundos', 'action' => 'view', $situacaoFundo->tipo_situacao_fundo->id]) : '' ?></td>
                    <td><?= h($situacaoFundo->DT_INI_SIT) ?></td>
                    <td><?= h($situacaoFundo->DT_REG_CVM) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $situacaoFundo->cnpj_fundo_id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $situacaoFundo->cnpj_fundo_id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $situacaoFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $situacaoFundo->cnpj_fundo_id)]) ?>
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
