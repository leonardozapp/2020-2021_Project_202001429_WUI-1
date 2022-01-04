<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * AdministradorFundo Entity
 *
 * @property int $id
 * @property string $cnpj
 * @property string $nome
 * @property \Cake\I18n\FrozenDate $DT_REG_CVM
 *
 * @property \App\Model\Entity\CadastroFundo[] $cadastro_fundos
 */
class AdministradorFundo extends Entity
{
    /**
     * Fields that can be mass assigned using newEntity() or patchEntity().
     *
     * Note that when '*' is set to true, this allows all unspecified fields to
     * be mass assigned. For security purposes, it is advised to set '*' to false
     * (or remove it), and explicitly make individual fields accessible as needed.
     *
     * @var array
     */
    protected $_accessible = [
        'cnpj' => true,
        'nome' => true,
        'DT_REG_CVM' => true,
        'cadastro_fundos' => true,
    ];
}
