<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\DocInfDiarioFundo[]|\Cake\Collection\CollectionInterface $docInfDiarioFundos
 */
?>
<div class="docInfDiarioFundos index content">
    <?= $this->Html->link(__('New Doc Inf Diario Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Doc Inf Diario Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('cnpj_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('DT_COMPTC') ?></th>
                    <th><?= $this->Paginator->sort('VL_TOTAL') ?></th>
                    <th><?= $this->Paginator->sort('VL_QUOTA') ?></th>
                    <th><?= $this->Paginator->sort('VL_PATRIM_LIQ') ?></th>
                    <th><?= $this->Paginator->sort('CAPTC_DIA') ?></th>
                    <th><?= $this->Paginator->sort('RESG_DIA') ?></th>
                    <th><?= $this->Paginator->sort('NR_COTST') ?></th>
                    <th><?= $this->Paginator->sort('rentab_diaria') ?></th>
                    <th><?= $this->Paginator->sort('volat_diaria') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($docInfDiarioFundos as $docInfDiarioFundo): ?>
                <tr>
                    <td><?= $docInfDiarioFundo->has('cnpj_fundo') ? $this->Html->link($docInfDiarioFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $docInfDiarioFundo->cnpj_fundo->id]) : '' ?></td>
                    <td><?= h($docInfDiarioFundo->DT_COMPTC) ?></td>
                    <td><?= $this->Number->format($docInfDiarioFundo->VL_TOTAL) ?></td>
                    <td><?= $this->Number->format($docInfDiarioFundo->VL_QUOTA) ?></td>
                    <td><?= $this->Number->format($docInfDiarioFundo->VL_PATRIM_LIQ) ?></td>
                    <td><?= $this->Number->format($docInfDiarioFundo->CAPTC_DIA) ?></td>
                    <td><?= $this->Number->format($docInfDiarioFundo->RESG_DIA) ?></td>
                    <td><?= $this->Number->format($docInfDiarioFundo->NR_COTST) ?></td>
                    <td><?= $this->Number->format($docInfDiarioFundo->rentab_diaria) ?></td>
                    <td><?= $this->Number->format($docInfDiarioFundo->volat_diaria) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $docInfDiarioFundo->cnpj_fundo_id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $docInfDiarioFundo->cnpj_fundo_id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $docInfDiarioFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $docInfDiarioFundo->cnpj_fundo_id)]) ?>
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
