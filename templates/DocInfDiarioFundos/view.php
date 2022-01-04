<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\DocInfDiarioFundo $docInfDiarioFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Doc Inf Diario Fundo'), ['action' => 'edit', $docInfDiarioFundo->cnpj_fundo_id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Doc Inf Diario Fundo'), ['action' => 'delete', $docInfDiarioFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $docInfDiarioFundo->cnpj_fundo_id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Doc Inf Diario Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Doc Inf Diario Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="docInfDiarioFundos view content">
            <h3><?= h($docInfDiarioFundo->cnpj_fundo_id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Cnpj Fundo') ?></th>
                    <td><?= $docInfDiarioFundo->has('cnpj_fundo') ? $this->Html->link($docInfDiarioFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $docInfDiarioFundo->cnpj_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('VL TOTAL') ?></th>
                    <td><?= $this->Number->format($docInfDiarioFundo->VL_TOTAL) ?></td>
                </tr>
                <tr>
                    <th><?= __('VL QUOTA') ?></th>
                    <td><?= $this->Number->format($docInfDiarioFundo->VL_QUOTA) ?></td>
                </tr>
                <tr>
                    <th><?= __('VL PATRIM LIQ') ?></th>
                    <td><?= $this->Number->format($docInfDiarioFundo->VL_PATRIM_LIQ) ?></td>
                </tr>
                <tr>
                    <th><?= __('CAPTC DIA') ?></th>
                    <td><?= $this->Number->format($docInfDiarioFundo->CAPTC_DIA) ?></td>
                </tr>
                <tr>
                    <th><?= __('RESG DIA') ?></th>
                    <td><?= $this->Number->format($docInfDiarioFundo->RESG_DIA) ?></td>
                </tr>
                <tr>
                    <th><?= __('NR COTST') ?></th>
                    <td><?= $this->Number->format($docInfDiarioFundo->NR_COTST) ?></td>
                </tr>
                <tr>
                    <th><?= __('Rentab Diaria') ?></th>
                    <td><?= $this->Number->format($docInfDiarioFundo->rentab_diaria) ?></td>
                </tr>
                <tr>
                    <th><?= __('Volat Diaria') ?></th>
                    <td><?= $this->Number->format($docInfDiarioFundo->volat_diaria) ?></td>
                </tr>
                <tr>
                    <th><?= __('DT COMPTC') ?></th>
                    <td><?= h($docInfDiarioFundo->DT_COMPTC) ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
